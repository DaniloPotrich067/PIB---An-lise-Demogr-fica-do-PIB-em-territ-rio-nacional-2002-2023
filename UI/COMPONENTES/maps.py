from __future__ import annotations

import json
import re
from pathlib import Path
import pandas as pd
import plotly.express as px
import streamlit as st

from COMPONENTES.shared import asset_path
from COMPONENTES.layout import fmt_int_br


UF_SIGLAS = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA","MT","MS","MG",
    "PA","PB","PR","PE","PI","RJ","RN","RS","RO","RR","SC","SP","SE","TO"
}
UF_IBGE_TO_SIGLA = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",
    21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",29:"BA",
    31:"MG",32:"ES",33:"RJ",35:"SP",
    41:"PR",42:"SC",43:"RS",
    50:"MS",51:"MT",52:"GO",53:"DF"
}
_SIGLA_RE = re.compile(r"\b([A-Z]{2})\b")


def _norm_sigla(x) -> str:
    if x is None:
        return ""
    s = str(x).strip().upper()
    if s in UF_SIGLAS:
        return s
    m = _SIGLA_RE.search(s.replace("-", " ").replace("_", " ").replace("/", " "))
    if m:
        cand = m.group(1)
        return cand if cand in UF_SIGLAS else ""
    return ""


def _try_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return None


def _detect_best_key(features: list[dict]) -> tuple[str | None, str]:
    if not features:
        return None, "geojson sem features"

    keys = set()
    for f in features:
        props = f.get("properties", {}) or {}
        keys.update(props.keys())

    best_key   = None
    best_score = -1
    best_mode  = "sigla"

    for k in keys:
        hits_sigla = 0
        hits_ibge  = 0
        for f in features:
            props = f.get("properties", {}) or {}
            v = props.get(k)
            s = _norm_sigla(v)
            if s in UF_SIGLAS:
                hits_sigla += 1
                continue
            n = _try_int(v)
            if n in UF_IBGE_TO_SIGLA:
                hits_ibge += 1

        score = hits_sigla * 100 + hits_ibge
        if score > best_score:
            best_score = score
            best_key   = k
            best_mode  = "sigla" if hits_sigla >= hits_ibge else "ibge"

    return best_key, best_mode


@st.cache_data(ttl=3600)
def _load_geojson_with_ids(geojson_file: str) -> tuple[dict, str]:
    geo_path = Path(asset_path(geojson_file))
    if not geo_path.exists():
        raise FileNotFoundError(f"GeoJSON não encontrado em: {geo_path}")

    with open(geo_path, "r", encoding="utf-8") as f:
        br = json.load(f)

    feats    = br.get("features", []) or []
    key, mode = _detect_best_key(feats)

    if key is None:
        for f in feats:
            f["id"] = ""
        return br, "nenhum"

    if mode == "sigla":
        for f in feats:
            props  = f.get("properties", {}) or {}
            f["id"] = _norm_sigla(props.get(key))
        return br, f"properties.{key} (sigla)"

    for f in feats:
        props  = f.get("properties", {}) or {}
        n      = _try_int(props.get(key))
        f["id"] = UF_IBGE_TO_SIGLA.get(n, "")
    return br, f"properties.{key} (ibge->sigla)"


# ── Classificação por média — mais intuitiva que tercis ───────────────────────

def _classify_media(v: pd.Series) -> tuple[pd.Series, dict]:
    """
    Classifica em 3 faixas usando média ± 0,5 desvio padrão.
    Mais intuitivo que tercis: o usuário entende imediatamente o que significa.
    """
    media = float(v.mean())
    std   = float(v.std())

    lim_baixo = media - 0.5 * std
    lim_alto  = media + 0.5 * std

    def lab(x: float) -> str:
        if x < lim_baixo:
            return "Abaixo da média"
        if x <= lim_alto:
            return "Na média"
        return "Acima da média"

    info = {
        "metodo":        "Classificação por média ± 0,5 desvio padrão",
        "media":         media,
        "desvio_padrao": std,
        "limite_baixo":  lim_baixo,
        "limite_alto":   lim_alto,
        "mediana":       float(v.median()),
        "min":           float(v.min()),
        "max":           float(v.max()),
    }
    return v.apply(lab), info


def choropleth_uf_faixas(
    df_uf: pd.DataFrame,
    *,
    value_col="pib",
    geojson_file="br_estados_simplified.geojson",
    opacity=0.82,
):
    if df_uf is None or df_uf.empty:
        return None, None, "Sem dados por UF para montar o mapa."

    if "sigla_uf" not in df_uf.columns or value_col not in df_uf.columns:
        return None, None, f"df_uf precisa ter colunas: sigla_uf e {value_col}"

    df = df_uf.copy()
    df["sigla_uf"] = df["sigla_uf"].map(_norm_sigla)
    df[value_col]  = df[value_col].astype(float)
    df = df[df["sigla_uf"].isin(UF_SIGLAS)]
    if df.empty:
        return None, None, "Sem UFs válidas após normalização."

    df["faixa"], info = _classify_media(df[value_col])
    info["contagem_por_faixa"] = df["faixa"].value_counts().to_dict()
    df["Valor (formato)"]      = df[value_col].apply(fmt_int_br)

    try:
        br, id_source = _load_geojson_with_ids(geojson_file)
    except Exception as e:
        return None, info, str(e)

    feats   = br.get("features", []) or []
    geo_ids = {_norm_sigla(ft.get("id")) for ft in feats}
    df_ids  = set(df["sigla_uf"].unique())
    matches = len(df_ids.intersection(geo_ids))

    info.update({
        "id_source":     id_source,
        "ufs_esperadas": len(df_ids),
        "matches":       matches,
        "ufs_sem_match": sorted(list(df_ids - geo_ids)),
    })

    if matches < len(df_ids):
        return None, info, (
            f"GeoJSON não casou com todas as UFs (matches={matches}/{len(df_ids)}). "
            f"Sem match: {', '.join(info['ufs_sem_match'][:10])}"
        )

    fig = px.choropleth_mapbox(
        df,
        geojson=br,
        locations="sigla_uf",
        featureidkey="id",
        color="faixa",
        hover_name="sigla_uf",
        hover_data={
            "Valor (formato)": True,
            "faixa":           True,
            value_col:         False,
            "sigla_uf":        False,
        },
        labels={"sigla_uf": "UF", "Valor (formato)": "Valor", "faixa": "Classificação"},
        color_discrete_map={
            "Abaixo da média": "#e74c3c",
            "Na média":        "#f1c40f",
            "Acima da média":  "#2ecc71",
        },
        category_orders={"faixa": ["Abaixo da média", "Na média", "Acima da média"]},
        # ── centro do Brasil
        mapbox_style="carto-darkmatter",
        center={"lat": -14.0, "lon": -51.0},
        zoom=3.0,
        opacity=float(opacity),
    )

    fig.update_layout(
        margin=dict(l=0, r=0, t=0, b=0),
        template="plotly_dark",
        legend_title_text="Classificação",
        mapbox=dict(
            style="carto-darkmatter",
            center={"lat": -14.0, "lon": -51.0},
            zoom=3.0,
        ),
    )

    return fig, info, None
