from __future__ import annotations

import os
import json
import time
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class ExtractResult:
    ok: bool
    path: Optional[str]
    status_code: Optional[int]
    error: Optional[str]


# =====================================================
# Utils
# =====================================================

def _safe_mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _write_json(path: str, payload: Any) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _resolve_out_dir(base_dir: str, prefix: str, endpoint_label: str) -> str:
    base  = os.path.join(base_dir, "raw")
    pre   = prefix.lower()
    label = endpoint_label.lower()

    if "localidades" in pre:
        for sub in ("regioes", "ufs", "municipios"):
            if sub in label:
                return os.path.join(base, "localidades", sub)
        return os.path.join(base, "localidades")

    if "sidra" in pre:
        ano = None
        for part in label.split("_"):
            if part.startswith("ano") and part[3:].isdigit() and len(part[3:]) == 4:
                ano = part[3:]
                break
        if ano:
            return os.path.join(base, "sidra", "pib_municipal", ano)
        return os.path.join(base, "sidra")

    return os.path.join(base, "misc")


def _build_filename(prefix: str, endpoint_label: str) -> str:
    pre   = prefix.lower()
    label = endpoint_label.lower()

    if "localidades" in pre:
        return f"{endpoint_label}.json"

    if "sidra" in pre:
        for part in label.split("_"):
            if part.startswith("v") and part[1:].isdigit():
                return f"pib_{part}.json"

    return f"{endpoint_label}.json"


def _is_valid_json(path: str) -> bool:
    """
    Retorna True só se o arquivo:
    - Existe em disco
    - É um JSON válido e parseável
    - Não está vazio ({} ou [])
    - No caso SIDRA, tem mais de 1 linha (linha 0 = só header)
    """
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not data:
            return False
        if isinstance(data, list) and len(data) <= 1:
            return False
        return True
    except (json.JSONDecodeError, OSError):
        return False


# =====================================================
# HTTP robusto
# =====================================================

def _get_json(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = 60,
    retries: int = 5,
    base_delay: float = 1.0,
    session: requests.Session | None = None,
) -> tuple[Any, int]:

    s = session or requests.Session()
    last_exc: Exception | None = None

    for attempt in range(retries + 1):
        try:
            resp   = s.get(url, params=params, headers=headers, timeout=timeout)
            status = resp.status_code

            if status >= 500:
                raise requests.exceptions.HTTPError(f"Erro servidor: {status}")
            if status == 429:
                raise requests.exceptions.HTTPError("Rate limit 429")

            resp.raise_for_status()

            ct = resp.headers.get("Content-Type", "")
            if "application/json" not in ct.lower():
                raise ValueError(f"Resposta nao JSON (Content-Type={ct})")

            data = resp.json()

            if isinstance(data, list) and data:
                if isinstance(data[0], dict) and "erro" in data[0]:
                    raise ValueError(f"Erro SIDRA: {data[0].get('erro')}")

            return data, status

        except Exception as e:
            last_exc = e
            if attempt >= retries:
                break
            time.sleep(base_delay * (2 ** attempt))

    raise last_exc  # type: ignore[misc]


# =====================================================
# Extract principal
# =====================================================

def extract_json(
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    prefix: str = "misc",
    endpoint_label: str = "geral",
    out_dir: str = "DATA",
    throttle_delay: float = 0.5,
) -> ExtractResult:

    try:
        final_dir = _resolve_out_dir(out_dir, prefix, endpoint_label)
        _safe_mkdir(final_dir)

        filename = _build_filename(prefix, endpoint_label)
        path     = os.path.join(final_dir, filename)

        # Arquivo válido em disco → pula requisição (retomada automática)
        if _is_valid_json(path):
            return ExtractResult(ok=True, path=path, status_code=None, error=None)

        # Arquivo inválido ou corrompido → remove antes de rebaixar
        if os.path.exists(path):
            os.remove(path)

        with requests.Session() as session:
            payload, status = _get_json(
                url,
                params=params,
                headers=headers,
                session=session,
            )

        time.sleep(throttle_delay)
        _write_json(path, payload)

        return ExtractResult(ok=True, path=path, status_code=status, error=None)

    except Exception as e:
        return ExtractResult(ok=False, path=None, status_code=None, error=str(e))


# Compatibilidade retroativa
def extract(
    url: str,
    params: dict | None = None,
    headers: dict | None = None,
    prefix: str = "misc",
    endpoint_label: str = "geral",
    out_dir: str = "DATA",
) -> str | None:
    res = extract_json(
        url,
        params=params,
        headers=headers,
        prefix=prefix,
        endpoint_label=endpoint_label,
        out_dir=out_dir,
    )
    return res.path if res.ok else None
