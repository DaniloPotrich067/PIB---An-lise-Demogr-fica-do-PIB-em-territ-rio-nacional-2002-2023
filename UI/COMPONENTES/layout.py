from __future__ import annotations

FRIENDLY_VARIABLES = {
    "Produto Interno Bruto a preços correntes": "PIB",
    "Produto Interno Bruto a preços correntes ": "PIB",
}

def apply_style():
    import streamlit as st
    st.markdown(
        """
        <style>
          .block-container { padding-top: 1.0rem; padding-bottom: 2.0rem; }
          [data-testid="stMetricValue"] { font-size: 1.55rem; }
          [data-testid="stMetricLabel"] { opacity: 0.86; }
          .hint { opacity: 0.78; font-size: 0.95rem; }
          .card {
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 12px;
            padding: 12px 14px;
            background: rgba(255,255,255,0.03);
          }
        </style>
        """,
        unsafe_allow_html=True,
    )

def fmt_int_br(x) -> str:
    try:
        n = float(x)
    except Exception:
        return str(x)
    s = f"{n:,.0f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_short_br(x) -> str:
    try:
        n = float(x)
    except Exception:
        return str(x)

    sign = "-" if n < 0 else ""
    n = abs(n)

    if n < 1_000:
        return f"{sign}{fmt_int_br(n)}"
    if n < 1_000_000:
        return f"{sign}{(n/1_000):.2f}".replace(".", ",") + "k"
    if n < 1_000_000_000:
        return f"{sign}{(n/1_000_000):.2f}".replace(".", ",") + "M"
    return f"{sign}{(n/1_000_000_000):.2f}".replace(".", ",") + "B"

def kpi_help_unidade():
    return (
        "Dica: a unidade real (ex.: R$ mil) vem da sua dim_variavel. "
        "Se quiser ficar 100% amigável, padronize no ETL (ex.: converter para R$)."
    )
