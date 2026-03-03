from __future__ import annotations

import streamlit as st
from COMPONENTES.maps import choropleth_uf_faixas
from COMPONENTES.layout import fmt_int_br


def render_uf_map_with_info(
    df_uf_map,
    *,
    value_col: str = "pib",
    opacity: float = 0.82,
    title: str = "Mapa — PIB por UF",
):
    st.subheader(title)

    if df_uf_map is None or df_uf_map.empty:
        st.warning("Sem dados por UF para o recorte selecionado.")
        return

    col_map, col_info = st.columns([1.6, 0.8])

    with col_map:
        fig, info, err = choropleth_uf_faixas(df_uf_map, value_col=value_col, opacity=opacity)
        if err:
            st.info(err)
        else:
            st.plotly_chart(fig, use_container_width=True)

    with col_info:
        st.subheader("Leitura do mapa")
        if not info:
            st.caption("Sem informações auxiliares.")
            return

        st.markdown(
            f"""
<div class="card">
<b>{info.get('metodo', '')}</b><br><br>
Média: {fmt_int_br(info.get('media', 0))}<br>
Desvio padrão: {fmt_int_br(info.get('desvio_padrao', 0))}<br>
Limite inferior: {fmt_int_br(info.get('limite_baixo', 0))}<br>
Limite superior: {fmt_int_br(info.get('limite_alto', 0))}<br>
Mediana: {fmt_int_br(info.get('mediana', 0))}<br>
Mín: {fmt_int_br(info.get('min', 0))}<br>
Máx: {fmt_int_br(info.get('max', 0))}<br>
</div>
""",
            unsafe_allow_html=True,
        )

        c = info.get("contagem_por_faixa", {}) or {}
        st.markdown(
            f"""
**UFs por faixa**
- Abaixo da média: {c.get('Abaixo da média', 0)}
- Na média: {c.get('Na média', 0)}
- Acima da média: {c.get('Acima da média', 0)}
"""
        )
