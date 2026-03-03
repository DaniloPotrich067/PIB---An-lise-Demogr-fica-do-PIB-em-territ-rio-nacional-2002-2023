from __future__ import annotations

import numpy as np
import streamlit as st
import plotly.graph_objects as go

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style, fmt_short_br
from COMPONENTES.filters import sidebar_filters, ANO_PADRAO
from COMPONENTES.data import query_base_municipios, query_pib_uf
from COMPONENTES.charts import hist_pib, box_pib_regiao
from COMPONENTES.blocks import render_uf_map_with_info

ensure_ui_in_path()
apply_style()

st.title("Distribuição — PIB municipal")
st.markdown("Desigualdade e dispersão do PIB entre municípios.")

conn = st.connection("pib", type="sql")
df_reg, df_uf, df_var, anos = load_dims(conn)

# Distribuição usa apenas PIB total — aceita 2022/2023, mas avisa
flt = sidebar_filters(df_reg, df_uf, df_var, anos, title="Filtros (Distribuição)", with_map=True)
usar_log = st.sidebar.checkbox("Escala logarítmica (recomendado)", value=True)

if flt["ano"] > ANO_PADRAO:
    st.info(
        f"⚠️ **{flt['ano']}** possui apenas PIB total — sem VAB setorial. "
        f"Análise de distribuição funciona, mas filtrar por setor não é possível. "
        f"Recomenda-se usar até **{ANO_PADRAO}** para análise completa.",
        icon="ℹ️",
    )

df = query_base_municipios(conn, flt)
if df.empty:
    st.warning("Sem dados para os filtros selecionados.")
    st.stop()

total   = float(df["valor"].sum())
q50     = float(df["valor"].quantile(0.50))
q90     = float(df["valor"].quantile(0.90))
q99     = float(df["valor"].quantile(0.99))


def gini(series):
    s = series.dropna().sort_values().values
    n = len(s)
    if n == 0 or s.sum() == 0:
        return 0.0
    return (2 * sum((i + 1) * v for i, v in enumerate(s)) / (n * s.sum())) - (n + 1) / n


gini_val = gini(df["valor"])

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("PIB (soma)", fmt_short_br(total))
c2.metric("Mediana", fmt_short_br(q50),
          help="50% dos municípios têm PIB abaixo deste valor")
c3.metric("Percentil 90", fmt_short_br(q90),
          help="90% dos municípios têm PIB abaixo deste valor")
c4.metric("Percentil 99", fmt_short_br(q99),
          help="99% dos municípios têm PIB abaixo deste valor")
c5.metric("Coeficiente de Gini", f"{gini_val:.3f}",
          help="0 = distribuição totalmente igualitária | 1 = toda riqueza num único município. Acima de 0,6 indica concentração severa.")

st.divider()

# ── Histograma ────────────────────────────────────────────────────────────────
st.subheader("Histograma — como os municípios se distribuem por faixa de PIB")
st.caption(
    "Cada barra mostra quantos municípios têm PIB naquela faixa. "
    "A escala logarítmica é necessária pois a diferença entre o menor e o maior município "
    "chega a 100.000x."
)
st.plotly_chart(hist_pib(df, use_log=usar_log), use_container_width=True, key="hist_distribuicao")

st.divider()

# ── Boxplot ───────────────────────────────────────────────────────────────────
st.subheader("Distribuição por região")

with st.expander("ℹ️ Como ler este gráfico"):
    st.markdown("""
**O gráfico de caixa (boxplot) resume a distribuição dos municípios de cada região:**

| Elemento | O que significa |
|---|---|
| **Linha central** | **Mediana** — metade dos municípios está acima, metade abaixo |
| **Borda superior da caixa** | **3º quartil (Q3)** — 75% dos municípios têm PIB abaixo deste valor |
| **Borda inferior da caixa** | **1º quartil (Q1)** — 25% dos municípios têm PIB abaixo deste valor |
| **Altura da caixa** | **Amplitude interquartil** — quanto varia o "miolo" da distribuição |
| **Linhas finas (bigodes)** | Faixa geral dos valores típicos |

**Caixa estreita** = municípios parecidos entre si.  
**Caixa alta** = muita desigualdade dentro da região.  
**Sudeste com caixa mais larga** reflete a coexistência de São Paulo e municípios pequenos no mesmo grupo.
    """)

st.plotly_chart(box_pib_regiao(df, use_log=usar_log), use_container_width=True, key="box_distribuicao")

st.divider()

# ── Lorenz ────────────────────────────────────────────────────────────────────
st.subheader("Curva de Lorenz")
st.caption(
    "Mostra como o PIB se acumula conforme ordenamos os municípios do menor para o maior. "
    "Quanto mais a linha azul se afasta da diagonal pontilhada, maior a desigualdade."
)

s        = df["valor"].dropna().sort_values().values
lorenz_y = np.cumsum(s) / s.sum()
lorenz_x = np.arange(1, len(s) + 1) / len(s)

fig_lorenz = go.Figure()
fig_lorenz.add_trace(go.Scatter(
    x=lorenz_x, y=lorenz_y, mode="lines",
    name="Distribuição real",
    line=dict(color="#1f77b4", width=2),
    hovertemplate="Top %{x:.1%} dos municípios detém %{y:.1%} do PIB<extra></extra>",
))
fig_lorenz.add_trace(go.Scatter(
    x=[0, 1], y=[0, 1], mode="lines",
    name="Igualdade perfeita",
    line=dict(color="gray", dash="dot"),
))
fig_lorenz.update_layout(
    template="plotly_dark",
    hovermode="x unified",
    xaxis_title="Fração acumulada de municípios (do menor para o maior PIB)",
    yaxis_title="Fração acumulada do PIB total",
    xaxis_tickformat=".0%",
    yaxis_tickformat=".0%",
)
st.plotly_chart(fig_lorenz, use_container_width=True, key="lorenz_distribuicao")

st.divider()

# ── Mapa ──────────────────────────────────────────────────────────────────────
from COMPONENTES.data import query_pib_uf as _query_pib_uf
render_uf_map_with_info(
    _query_pib_uf(conn, flt),
    value_col="pib",
    opacity=flt["map_opacity"],
    title="Mapa — PIB por UF",
)
