from __future__ import annotations

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style, fmt_short_br
from COMPONENTES.filters import sidebar_filters
from COMPONENTES.data import query_serie_historica

ensure_ui_in_path()
apply_style()

st.title("Evolução Temporal — PIB e setores")
st.markdown("Como o PIB e cada setor evoluíram de 2002 a 2023.")

conn = st.connection("pib", type="sql")
df_reg, df_uf, df_var, anos = load_dims(conn)
flt = sidebar_filters(
    df_reg, df_uf, df_var, anos,
    title="Filtros (Temporal)",
    with_ano_range=True,
    with_uf_single=True,
)

df = query_serie_historica(conn, flt)
if df.empty:
    st.warning("Sem dados para os filtros selecionados.")
    st.stop()

LABELS = {
    "pib": "PIB Total", "vab_agro": "Agropecuária",
    "vab_ind": "Indústria", "vab_serv": "Serviços",
    "vab_apsp": "Adm. Pública", "impostos": "Impostos",
}
COLS = ["pib", "vab_agro", "vab_ind", "vab_serv", "vab_apsp"]

# KPIs
ano_ini_val = int(df["ano"].min())
ano_fim_val = int(df["ano"].max())
pib_ini = float(df[df["ano"] == ano_ini_val]["pib"].sum())
pib_fim = float(df[df["ano"] == ano_fim_val]["pib"].sum())
cresc   = (pib_fim - pib_ini) / pib_ini if pib_ini else 0.0

c1, c2, c3 = st.columns(3)
c1.metric(f"PIB {ano_ini_val}", fmt_short_br(pib_ini))
c2.metric(f"PIB {ano_fim_val}", fmt_short_br(pib_fim))
c3.metric("Crescimento acumulado", f"{cresc:+.1%}")

st.divider()

# Linha — valores absolutos
st.subheader("PIB e setores — valores absolutos")
df_melt = df.melt(id_vars=["ano"], value_vars=COLS, var_name="serie", value_name="valor")
df_melt["serie"] = df_melt["serie"].map(LABELS)

fig_abs = px.line(
    df_melt, x="ano", y="valor", color="serie",
    markers=True, template="plotly_dark",
    labels={"ano": "Ano", "valor": "R$ mil", "serie": ""},
)
fig_abs.update_layout(hovermode="x unified")
st.plotly_chart(fig_abs, use_container_width=True)

st.divider()

# Base 100
st.subheader("Crescimento relativo — base 100 no ano inicial")
st.caption("Mostra qual setor cresceu mais independente do tamanho absoluto.")

df_idx = df[["ano"] + COLS].copy()
for col in COLS:
    base = df_idx.loc[df_idx["ano"] == df_idx["ano"].min(), col].values
    if len(base) > 0 and base[0] != 0:
        df_idx[col] = df_idx[col] / base[0] * 100

df_melt2 = df_idx.melt(id_vars=["ano"], value_vars=COLS, var_name="serie", value_name="indice")
df_melt2["serie"] = df_melt2["serie"].map(LABELS)

fig_idx = px.line(
    df_melt2, x="ano", y="indice", color="serie",
    markers=True, template="plotly_dark",
    labels={"ano": "Ano", "indice": "Índice (base 100)", "serie": ""},
)
fig_idx.add_hline(y=100, line_dash="dot", line_color="gray", annotation_text="Base inicial")
fig_idx.update_layout(hovermode="x unified")
st.plotly_chart(fig_idx, use_container_width=True)

st.divider()

# Variação % anual
st.subheader("Variação % anual — PIB")
df_var_anual = df[["ano", "pib"]].copy()
df_var_anual["var_pct"] = df_var_anual["pib"].pct_change() * 100

fig_var = px.bar(
    df_var_anual.dropna(), x="ano", y="var_pct",
    color="var_pct", color_continuous_scale=["#d62728", "#aaa", "#2ca02c"],
    template="plotly_dark",
    labels={"ano": "Ano", "var_pct": "Variação % anual"},
)
fig_var.add_hline(y=0, line_dash="dot", line_color="gray")
fig_var.update_layout(coloraxis_showscale=False)
st.plotly_chart(fig_var, use_container_width=True)
