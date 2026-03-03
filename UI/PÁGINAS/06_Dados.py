from __future__ import annotations

import streamlit as st
import plotly.express as px

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style
from COMPONENTES.filters import sidebar_filters
from COMPONENTES.data import (
    query_base_municipios, query_sanity_counts,
    query_missing_municipios_por_uf, query_total_municipios,
)

ensure_ui_in_path()
apply_style()

st.title("Dados & Qualidade")
st.markdown("Cobertura e consistência do banco para evitar análise enganosa.")

conn = st.connection("pib", type="sql")
df_reg, df_uf, df_var, anos = load_dims(conn)
flt = sidebar_filters(df_reg, df_uf, df_var, anos, title="Filtros (Dados & Qualidade)", with_city=True)
limit = st.sidebar.slider("Linhas (amostra)", 10, 200, 50, 10)

san = query_sanity_counts(conn)
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Regiões",    int(san["n_regiao"].iloc[0]))
c2.metric("UFs",        int(san["n_uf"].iloc[0]))
c3.metric("Municípios", int(san["n_municipio"].iloc[0]))
c4.metric("Variáveis",  int(san["n_variavel"].iloc[0]))
c5.metric("Fatos",      int(san["n_fato"].iloc[0]))

st.divider()

df_base = query_base_municipios(conn, flt)
n_com_dado = int(df_base["id_municipio"].nunique()) if not df_base.empty else 0
tot_mun    = query_total_municipios(conn)
cob        = (n_com_dado / tot_mun) if tot_mun else 0.0
st.metric("Cobertura (municípios com dado no recorte)", f"{cob:.1%}")

st.subheader("Municípios sem dado (por UF) — diagnóstico")
df_missing = query_missing_municipios_por_uf(conn, flt)
st.dataframe(df_missing, use_container_width=True)

if not df_missing.empty:
    fig = px.bar(df_missing.head(15), x="sigla_uf", y="municipios_sem_pib", template="plotly_dark")
    fig.update_layout(xaxis_title=None, yaxis_title=None)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

st.subheader("Amostra filtrada (inspeção)")
if df_base.empty:
    st.info("Amostra vazia para os filtros.")
else:
    st.dataframe(
        df_base.sort_values("valor", ascending=False)
               .head(limit)[["sigla_regiao", "sigla_uf", "nome_municipio", "valor"]],
        use_container_width=True,
    )
