from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="PIB Municipal", layout="wide")

from COMPONENTES.shared import ensure_ui_in_path
from COMPONENTES.layout import apply_style

ensure_ui_in_path()
apply_style()

pg = st.navigation([
    st.Page("PÁGINAS/00_Extrair.py",   title="Extrair",       icon=":material/info:"),
    st.Page("PÁGINAS/01_Dashboard.py",    title="Dashboard",        icon=":material/dashboard:"),
    st.Page("PÁGINAS/02_Temporal.py",     title="Análise Temporal", icon=":material/timeline:"),
    st.Page("PÁGINAS/03_Composicao.py",   title="Composição",       icon=":material/donut_small:"),
    st.Page("PÁGINAS/04_Distribuicao.py", title="Distribuição",     icon=":material/insights:"),
    st.Page("PÁGINAS/05_Concentracao.py", title="Concentração",     icon=":material/pie_chart:"),
    st.Page("PÁGINAS/06_Dados.py",        title="Dados & Qualidade",icon=":material/table_view:"),
])

pg.run()
