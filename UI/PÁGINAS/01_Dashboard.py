from __future__ import annotations

import streamlit as st

from COMPONENTES.shared import ensure_ui_in_path, load_dims
from COMPONENTES.layout import apply_style, fmt_short_br, kpi_help_unidade
from COMPONENTES.filters import sidebar_filters
from COMPONENTES.data import query_base_municipios, query_valor_por_uf, query_total_municipios
from COMPONENTES.charts import bar_top_municipios, bar_top_ufs
from COMPONENTES.blocks import render_uf_map_with_info

ensure_ui_in_path()
apply_style()

st.title("Dashboard — PIB municipal")
st.markdown("Visão executiva do recorte: tamanho, cobertura, mapa e maiores municípios/UFs.")

conn = st.connection("pib", type="sql")

try:
    df_reg, df_uf, df_var, anos = load_dims(conn)
    if not anos:
        st.error("❌ Nenhum dado carregado no banco. Verifique a conexão Neon.")
        st.stop()
except Exception as e:
    st.error(f"❌ Erro ao carregar dimensões: {str(e)}")
    st.stop()

flt = sidebar_filters(
    df_reg, df_uf, df_var, anos,
    title="Filtros (Dashboard)",
    with_top_n=True,
    with_map=True,
    with_var=True,
)

try:
    df = query_base_municipios(conn, flt)
    if df.empty:
        st.warning("⚠️ Sem dados para os filtros selecionados.")
        st.stop()
except Exception as e:
    st.error(f"❌ Erro ao carregar dados: {str(e)}")
    st.stop()

total      = float(df["valor"].sum())
n_mun      = int(df["id_municipio"].nunique())
n_uf       = int(df["sigla_uf"].nunique())
n_reg      = int(df["sigla_regiao"].nunique())
tot_mun    = query_total_municipios(conn)
cob        = (n_mun / tot_mun) if tot_mun else 0.0
df_sorted  = df.sort_values("valor", ascending=False)
top1       = df_sorted.iloc[0]
top1_share = float(top1["valor"]) / total if total else 0.0
top5_share = float(df_sorted.head(5)["valor"].sum()) / total if total else 0.0

delta_pib, delta_cor = None, "normal"
ano_atual = flt.get("ano")
if ano_atual and ano_atual > min(anos):
    try:
        df_ant = query_base_municipios(conn, {**flt, "ano": ano_atual - 1})
        if not df_ant.empty:
            total_ant = float(df_ant["valor"].sum())
            var = (total - total_ant) / total_ant if total_ant else 0.0
            delta_pib = f"{var:+.1%} vs {ano_atual - 1}"
            delta_cor = "normal" if var >= 0 else "inverse"
    except Exception:
        pass

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("Valor (soma)", fmt_short_br(total), delta=delta_pib,
          delta_color=delta_cor, help=kpi_help_unidade())
c2.metric("Municípios c/ dado", f"{n_mun:,}".replace(",", "."))
c3.metric("Cobertura", f"{cob:.1%}")
c4.metric("UFs", str(n_uf))
c5.metric("Regiões", str(n_reg))
c6.metric("Top1 share", f"{top1_share:.1%}",
          help=f"**Maior:** {top1['nome_municipio']} ({top1['sigla_uf']})\n\n**Top 5 acumulado:** {top5_share:.1%}")

st.divider()

try:
    df_uf_val = query_valor_por_uf(conn, flt)
    if not df_uf_val.empty:
        render_uf_map_with_info(df_uf_val, value_col="pib", opacity=flt.get("map_opacity", 0.7))
    else:
        st.warning("⚠️ Sem dados de UF para o mapa.")
except Exception as e:
    st.warning(f"⚠️ Erro ao carregar mapa: {str(e)}")

st.divider()

left, right = st.columns([1.2, 1.0])

with left:
    try:
        st.subheader(f"Top {flt.get('top_n', 10)} municípios — {flt.get('var_label', '')}")
        st.plotly_chart(
            bar_top_municipios(df, n=flt.get("top_n", 10), color_col="sigla_regiao"),
            use_container_width=True,
        )
    except Exception as e:
        st.error(f"❌ Erro ao renderizar gráfico de municípios: {str(e)}")

with right:
    try:
        st.subheader("Top 10 UFs (soma)")
        df_uf_agg = df.groupby("sigla_uf", as_index=False)["valor"].sum().rename(columns={"valor": "pib"})
        st.plotly_chart(bar_top_ufs(df_uf_agg, n=10), use_container_width=True)
    except Exception as e:
        st.error(f"❌ Erro ao renderizar gráfico de UFs: {str(e)}")

st.divider()

st.caption(
    f"📌 **{top1['nome_municipio']} ({top1['sigla_uf']})** lidera com "
    f"**{top1_share:.1%}** do valor total do recorte. "
    f"Os 5 maiores municípios concentram **{top5_share:.1%}** do total."
)
