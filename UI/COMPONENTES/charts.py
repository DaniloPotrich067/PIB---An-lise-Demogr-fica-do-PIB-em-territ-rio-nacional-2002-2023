from __future__ import annotations

import numpy as np
import plotly.express as px
import plotly.graph_objects as go

# Rótulos amigáveis usados em todos os gráficos
LABELS = {
    "sigla_uf":       "UF",
    "sigla_regiao":   "Região",
    "nome_municipio": "Município",
    "valor":          "PIB (R$ mil)",
    "pib":            "PIB (R$ mil)",
    "vab_agro":       "Agropecuária (R$ mil)",
    "vab_ind":        "Indústria (R$ mil)",
    "vab_serv":       "Serviços (R$ mil)",
    "vab_apsp":       "Adm. Pública (R$ mil)",
    "impostos":       "Impostos (R$ mil)",
    "pct_agro":       "Agropecuária (%)",
    "pct_ind":        "Indústria (%)",
    "pct_serv":       "Serviços (%)",
    "pct_apsp":       "Adm. Pública (%)",
    "top1_share":     "Share do maior município",
    "top10_share":    "Share dos 10 maiores",
    "hhi":            "Índice HHI",
    "n_municipios":   "Nº de municípios",
    "ano":            "Ano",
    "serie":          "Indicador",
    "indice":         "Índice (base 100)",
    "var_pct":        "Variação anual (%)",
    "setor":          "Setor",
    "pct":            "Participação (%)",
}

REGIOES = {"N": "Norte", "NE": "Nordeste", "SE": "Sudeste", "S": "Sul", "CO": "Centro-Oeste"}


def _fmt_regiao(df, col="sigla_regiao"):
    """Substitui sigla por nome completo da região no dataframe."""
    df = df.copy()
    if col in df.columns:
        df[col] = df[col].map(lambda x: REGIOES.get(x, x))
    return df


def bar_top_municipios(df, n: int = 10, color_col: str = "sigla_regiao"):
    top = _fmt_regiao(df.sort_values("valor", ascending=False).head(n))
    fig = px.bar(
        top, x="nome_municipio", y="valor",
        color=color_col,
        template="plotly_dark",
        labels=LABELS,
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="PIB (R$ mil)",
        legend_title="Região",
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>PIB: R$ %{y:,.0f} mil<extra></extra>"
    )
    return fig


def bar_top_ufs(df_uf, n: int = 10):
    top = df_uf.sort_values("pib", ascending=False).head(n)
    fig = px.bar(
        top, x="sigla_uf", y="pib",
        template="plotly_dark",
        labels=LABELS,
        color="pib",
        color_continuous_scale="Blues",
    )
    fig.update_layout(
        xaxis_title=None,
        yaxis_title="PIB (R$ mil)",
        coloraxis_showscale=False,
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>PIB: R$ %{y:,.0f} mil<extra></extra>"
    )
    return fig


def hist_pib(df, use_log: bool = True):
    """
    Histograma correto em escala log:
    transforma os dados com log10 antes de plotar —
    usar log_x=True no px.histogram muda o binning e distorce o resultado.
    """
    df2 = _fmt_regiao(df[df["valor"] > 0].copy())

    if use_log:
        df2["valor_plot"] = np.log10(df2["valor"])
        x_col = "valor_plot"
        # Ticks formatados: mostra o valor original no eixo
        tick_vals = [0, 1, 2, 3, 4, 5, 6]
        tick_text = ["R$ 1", "R$ 10", "R$ 100", "R$ 1k", "R$ 10k", "R$ 100k", "R$ 1M"]
        x_title = "PIB (R$ mil, escala logarítmica)"
    else:
        df2["valor_plot"] = df2["valor"]
        x_col = "valor_plot"
        tick_vals = None
        tick_text = None
        x_title = "PIB (R$ mil)"

    fig = px.histogram(
        df2, x=x_col, nbins=60,
        color="sigla_regiao",
        template="plotly_dark",
        labels={"valor_plot": x_title, "sigla_regiao": "Região"},
        barmode="overlay",
        opacity=0.8,
    )

    if use_log and tick_vals:
        fig.update_xaxes(tickvals=tick_vals, ticktext=tick_text)

    fig.update_layout(
        xaxis_title=x_title,
        yaxis_title="Nº de municípios",
        legend_title="Região",
        bargap=0.05,
    )
    return fig


def box_pib_regiao(df, use_log: bool = True):
    """
    Boxplot por região com rótulos explicativos.
    O boxplot mostra: mínimo, 1º quartil (25%), mediana (50%), 3º quartil (75%) e máximo.
    Pontos fora dos bigodes são outliers — municípios muito acima ou abaixo da faixa típica.
    """
    df2 = _fmt_regiao(df[df["valor"] > 0].copy())

    fig = px.box(
        df2,
        x="sigla_regiao", y="valor",
        points=False,
        template="plotly_dark",
        labels={"sigla_regiao": "Região", "valor": "PIB (R$ mil)"},
        color="sigla_regiao",
        color_discrete_sequence=px.colors.qualitative.Plotly,
    )

    if use_log:
        fig.update_yaxes(type="log", title="PIB (R$ mil, escala log)")

    fig.update_layout(
        xaxis_title=None,
        showlegend=False,
        hoverlabel=dict(bgcolor="#1e1e1e"),
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{x}</b><br>"
            "Máximo: %{upperfence:,.0f}<br>"
            "Q3 (75%): %{q3:,.0f}<br>"
            "Mediana: %{median:,.0f}<br>"
            "Q1 (25%): %{q1:,.0f}<br>"
            "Mínimo: %{lowerfence:,.0f}<extra></extra>"
        )
    )
    return fig
