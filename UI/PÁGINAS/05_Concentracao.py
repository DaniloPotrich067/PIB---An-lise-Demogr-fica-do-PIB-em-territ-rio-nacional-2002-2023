from __future__ import annotations

import streamlit as st
import plotly.express as px

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style
from COMPONENTES.filters import sidebar_filters
from COMPONENTES.data import query_concentracao_uf, query_pib_uf
from COMPONENTES.blocks import render_uf_map_with_info

ensure_ui_in_path()
apply_style()

st.title("Concentração — PIB municipal dentro da UF")
st.markdown(
    "Dependência de poucos municípios por UF:\n"
    "- **Top1 share:** participação do maior município.\n"
    "- **Top10 share:** participação acumulada dos 10 maiores.\n"
    "- **HHI:** índice Herfindahl-Hirschman (0–1, maior = mais concentrado)."
)

conn = st.connection("pib", type="sql")
df_reg, df_uf, df_var, anos = load_dims(conn)
flt = sidebar_filters(df_reg, df_uf, df_var, anos, title="Filtros (Concentração)", with_map=True)

render_uf_map_with_info(query_pib_uf(conn, flt), value_col="pib", opacity=flt["map_opacity"],
                        title="Mapa — PIB por UF (contexto do recorte)")

st.divider()

mostrar_hhi = st.sidebar.checkbox("Mostrar HHI (avançado)", value=False)

df = query_concentracao_uf(conn, flt)
if df.empty:
    st.warning("Sem dados para os filtros.")
    st.stop()

cols = ["sigla_uf", "n_municipios", "top1_share", "top10_share"] + (["hhi"] if mostrar_hhi else [])
st.subheader("Tabela (por UF)")
st.dataframe(df[cols], use_container_width=True)

st.subheader("Top1 share — mais dependente de 1 município")
fig1 = px.bar(
    df.sort_values("top1_share", ascending=False),
    x="sigla_uf", y="top1_share",
    color="top1_share", color_continuous_scale="Reds", template="plotly_dark",
)
fig1.update_layout(xaxis_title=None, yaxis_title=None, coloraxis_showscale=False)
st.plotly_chart(fig1, use_container_width=True)

st.subheader("Top10 share — mais concentrado nos 10 maiores")
fig10 = px.bar(
    df.sort_values("top10_share", ascending=False),
    x="sigla_uf", y="top10_share",
    color="top10_share", color_continuous_scale="Oranges", template="plotly_dark",
)
fig10.update_layout(xaxis_title=None, yaxis_title=None, coloraxis_showscale=False)
st.plotly_chart(fig10, use_container_width=True)

if mostrar_hhi:
    st.subheader("HHI — índice Herfindahl-Hirschman")
    fig_hhi = px.bar(
        df.sort_values("hhi", ascending=False),
        x="sigla_uf", y="hhi",
        color="hhi", color_continuous_scale="Purples", template="plotly_dark",
    )
    fig_hhi.update_layout(xaxis_title=None, yaxis_title=None, coloraxis_showscale=False)
    st.plotly_chart(fig_hhi, use_container_width=True)
