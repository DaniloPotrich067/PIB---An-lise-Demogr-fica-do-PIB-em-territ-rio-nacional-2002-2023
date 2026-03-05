from __future__ import annotations

from pathlib import Path
import re
import sys

import streamlit as st
import pandas as pd

from COMPONENTES.layout import FRIENDLY_VARIABLES

_SIDRA_PREFIX_RE = re.compile(r"^SIDRA:\s*\d+\s*-\s*")


def ui_root() -> Path:
    return Path(__file__).resolve().parents[1]


def ensure_ui_in_path() -> None:
    root = ui_root()
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))


def asset_path(*parts: str) -> str:
    return str(ui_root().joinpath("assets", *parts))


@st.cache_data(ttl=86400)
def load_dims(_conn):
    """Carrega dimensões estáticas (regiao, uf, variavel) + anos disponíveis. Cache de 24h."""
    try:
        df_reg = _conn.query("SELECT * FROM dim_regiao ORDER BY id_regiao;",     ttl="1h")
        df_uf  = _conn.query("SELECT * FROM dim_uf ORDER BY sigla_uf;",          ttl="1h")
        df_var = _conn.query("SELECT * FROM dim_variavel ORDER BY id_variavel;", ttl="1h")
        anos   = _conn.query(
            "SELECT DISTINCT ano FROM fato_indicador_municipio ORDER BY ano DESC;",
            ttl="1h",
        )["ano"].tolist()
        return df_reg, df_uf, df_var, anos
    except Exception as e:
        st.error(f"❌ Erro ao carregar dimensões: {str(e)}")
        raise


def uf_options_for_region(df_uf: pd.DataFrame, id_regiao: int | None) -> list[str]:
    if id_regiao is None:
        return df_uf["sigla_uf"].tolist()
    return df_uf.loc[df_uf["id_regiao"] == int(id_regiao), "sigla_uf"].tolist()


def sql_in_params(prefix: str, values: list[str], params: dict) -> str:
    ph = []
    for i, v in enumerate(values):
        k = f"{prefix}{i}"
        ph.append(f":{k}")
        params[k] = v
    return ", ".join(ph)


def _friendly_var_name(raw: str) -> str:
    base = _SIDRA_PREFIX_RE.sub("", (raw or "").strip())
    return FRIENDLY_VARIABLES.get(base, base)


def build_var_map(df_var: pd.DataFrame) -> dict[int, str]:
    out: dict[int, str] = {}
    for r in df_var.itertuples(index=False):
        k        = int(r.id_variavel)
        raw      = str(r.nome_variavel)
        friendly = _friendly_var_name(raw)
        unidade  = "" if pd.isna(r.unidade) or r.unidade is None else str(r.unidade).strip()
        out[k]   = f"{friendly} ({unidade})" if unidade else friendly
    return out
