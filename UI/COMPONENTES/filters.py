from __future__ import annotations

import streamlit as st
from COMPONENTES.shared import build_var_map, uf_options_for_region

# Último ano com série completa (setorial + PIB)
ANO_PADRAO = 2021


def _default_ano_idx(anos: list, preferido: int = ANO_PADRAO) -> int:
    """Aponta para o ano preferido; se não existir, vai para o último ≤ preferido."""
    anos_int = [int(a) for a in anos]
    if preferido in anos_int:
        return anos_int.index(preferido)
    candidatos = [i for i, a in enumerate(anos_int) if a <= preferido]
    return candidatos[-1] if candidatos else len(anos) - 1


def sidebar_filters(
    df_reg,
    df_uf,
    df_var,
    anos,
    *,
    title: str,
    with_city: bool = False,
    with_top_n: bool = False,
    with_map: bool = False,
    with_ano_range: bool = False,
    with_uf_single: bool = False,
):
    st.sidebar.header(title)

    if not anos:
        st.sidebar.error("Sem anos no fato. Rode o ETL.")
        st.stop()

    flt: dict = {}

    if with_ano_range:
        ano_min = int(min(anos))
        ano_max = int(max(anos))
        # Padrão: de 2002 até 2021 (último ano com série completa)
        fim_padrao = ANO_PADRAO if ANO_PADRAO <= ano_max else ano_max
        ano_range = st.sidebar.slider(
            "Período", ano_min, ano_max,
            (ano_min, fim_padrao),
        )
        flt["ano_ini"] = ano_range[0]
        flt["ano_fim"] = ano_range[1]
        flt["ano"]     = ano_range[1]
    else:
        idx = _default_ano_idx(anos)
        ano = st.sidebar.selectbox("Ano", options=anos, index=idx)
        flt["ano"]     = int(ano)
        flt["ano_ini"] = int(ano)
        flt["ano_fim"] = int(ano)

    var_map = build_var_map(df_var)
    id_variavel = st.sidebar.selectbox(
        "Indicador",
        options=list(var_map.keys()),
        format_func=lambda k: var_map[k],
    )

    id_regiao = st.sidebar.selectbox(
        "Região",
        options=[None] + df_reg["id_regiao"].astype(int).tolist(),
        format_func=lambda x: "Todas" if x is None
            else f"{x} — {df_reg.loc[df_reg.id_regiao == x, 'sigla_regiao'].iloc[0]}",
    )

    uf_opts = uf_options_for_region(df_uf, id_regiao)

    if with_uf_single:
        uf_sel = st.sidebar.selectbox("UF", options=["Todas"] + uf_opts)
        flt["uf_single"] = None if uf_sel == "Todas" else uf_sel
        flt["ufs"]       = [] if uf_sel == "Todas" else [uf_sel]
    else:
        sel_ufs = st.sidebar.multiselect("UF(s)", options=uf_opts, default=[])
        flt["ufs"]       = sel_ufs
        flt["uf_single"] = None

    cidade = ""
    if with_city:
        cidade = st.sidebar.text_input("Município (contém)", value="").strip()

    top_n = 10
    if with_top_n:
        top_n = st.sidebar.slider("Top N municípios", 5, 30, 10)

    map_opacity = 0.82
    if with_map:
        map_opacity = st.sidebar.slider("Transparência do mapa", 40, 100, 82) / 100.0

    flt.update({
        "id_variavel": int(id_variavel),
        "id_regiao":   None if id_regiao is None else int(id_regiao),
        "cidade":      cidade,
        "top_n":       int(top_n),
        "map_opacity": float(map_opacity),
        "var_label":   var_map[int(id_variavel)],
    })
    return flt
