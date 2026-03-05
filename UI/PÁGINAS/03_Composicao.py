from __future__ import annotations

import streamlit as st
import plotly.express as px

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style
from COMPONENTES.filters import sidebar_filters
from COMPONENTES.data import query_composicao_uf

ensure_ui_in_path()
apply_style()

st.title("Composição Setorial — estrutura do PIB")
st.markdown("De que é feito o PIB de cada UF: agropecuária, indústria, serviços e administração pública.")

conn = st.connection("pib", type="sql")
df_reg, df_uf, df_var, anos = load_dims(conn)
flt = sidebar_filters(df_reg, df_uf, df_var, anos, title="Filtros (Composição)")

df = query_composicao_uf(conn, flt)
if df.empty:
    st.warning("Sem dados para os filtros selecionados.")
    st.stop()

SETORES = {
    "pct_agro": "Agropecuária",
    "pct_ind":  "Indústria",
    "pct_serv": "Serviços",
    "pct_apsp": "Adm. Pública",
}
CORES = {
    "Agropecuária": "#2ca02c",
    "Indústria":    "#1f77b4",
    "Serviços":     "#ff7f0e",
    "Adm. Pública": "#9467bd",
}

st.subheader("Participação de cada setor por UF")
st.caption("Cada barra = 100% do VAB daquela UF dividido pelos setores.")

df_melt = df.melt(
    id_vars=["sigla_uf"], value_vars=list(SETORES.keys()),
    var_name="setor", value_name="pct"
)
df_melt["setor"] = df_melt["setor"].map(SETORES)

fig_stack = px.bar(
    df_melt, x="sigla_uf", y="pct", color="setor",
    barmode="stack", template="plotly_dark",
    color_discrete_map=CORES,
    labels={"sigla_uf": "UF", "pct": "%", "setor": "Setor"},
)
fig_stack.update_layout(yaxis_ticksuffix="%", hovermode="x unified", xaxis_title=None)
st.plotly_chart(fig_stack, use_container_width=True)

st.divider()

st.subheader("Heatmap — intensidade setorial por UF")
st.caption("Quanto mais intenso, maior a participação daquele setor no VAB da UF.")

df_heat = df[["sigla_uf"] + list(SETORES.keys())].rename(columns=SETORES).set_index("sigla_uf")
fig_heat = px.imshow(
    df_heat.T, color_continuous_scale="YlOrRd",
    template="plotly_dark", aspect="auto",
    labels={"x": "UF", "y": "Setor", "color": "%"},
)
st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

st.subheader("Composição de uma UF específica")
ufs_disp = sorted(df["sigla_uf"].tolist())
default_idx = ufs_disp.index("SP") if "SP" in ufs_disp else 0
uf_sel = st.selectbox("Selecione a UF", ufs_disp, index=default_idx)

row  = df[df["sigla_uf"] == uf_sel].iloc[0]
vals = {v: float(row[k]) for k, v in SETORES.items()}

fig_donut = px.pie(
    names=list(vals.keys()), values=list(vals.values()),
    hole=0.45, template="plotly_dark",
    color_discrete_map=CORES, color=list(vals.keys()),
)
fig_donut.update_traces(textinfo="label+percent")
fig_donut.update_layout(title=f"{uf_sel} — composição setorial do VAB")
st.plotly_chart(fig_donut, use_container_width=True)

with st.expander("Ver tabela completa"):
    # nome_uf vem do JOIN em query_composicao_uf
    cols_exib = [c for c in ["sigla_uf", "nome_uf", "pib", "vab_agro", "vab_ind", "vab_serv", "vab_apsp"] if c in df.columns]
    st.dataframe(df[cols_exib], use_container_width=True)
