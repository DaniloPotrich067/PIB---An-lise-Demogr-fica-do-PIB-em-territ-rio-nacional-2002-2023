from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ETL.extract import extract_json, ExtractResult
from ETL.transform import (
    transform_pib_sidra,
    transform_regioes,
    transform_ufs,
    transform_municipios,
)
from ETL.load import make_engine, load_all, sanity


# =========================================================
# CONFIG
# =========================================================

st.title("ETL — PIB Municipal (SIDRA / IBGE)")

IBGE_BASE   = "https://servicodados.ibge.gov.br/api/v1/localidades"
SIDRA_BASE  = "https://apisidra.ibge.gov.br/values"
SIDRA_TABLE = 5938
HEADERS     = {"Accept": "application/json"}
VARS_ATE_2021: set[int] = {498, 513, 517, 6575, 525, 543}

PERIODOS: dict[str, list[int]] = {
    "2002 a 2023 (serie completa)": list(range(2002, 2024)),
    "2009 a 2023 (15 anos)":        list(range(2009, 2024)),
    "2014 a 2023 (10 anos)":        list(range(2014, 2024)),
    "2019 a 2023 (5 anos)":         list(range(2019, 2024)),
    "2022 e 2023 (ultimos 2)":      [2022, 2023],
}

PRESET_VARS: dict[int, str] = {
    37:   "PIB a precos correntes (Mil R$)",
    498:  "VAB Total (Mil R$)",
    513:  "VAB Agropecuaria (Mil R$)",
    517:  "VAB Industria (Mil R$)",
    6575: "VAB Servicos (Mil R$)",
    525:  "VAB Adm. Publica (Mil R$)",
    543:  "Impostos liquidos (Mil R$)",
}

cfg    = st.secrets["connections"]["pib"]
engine = make_engine(
    user=cfg["username"],
    password=cfg["password"],
    host=cfg["host"],
    port=int(cfg["port"]),
    database=cfg["database"],
)


# =========================================================
# AVISO SIDRA — limitação 2022/2023
# =========================================================

st.info(
    "**Limitação de dados SIDRA/IBGE:**  \n"
    "A tabela 5938 do SIDRA publica dados municipais completos (PIB + VAB setorial) apenas até **2021**.  \n"
    "Para **2022 e 2023**, o IBGE disponibiliza **somente PIB total e PIB per capita** — "
    "o VAB de Agropecuária, Indústria, Serviços, Adm. Pública e Impostos **não foram publicados** para esses anos.  \n"
    "As combinações afetadas são automaticamente ignoradas durante a extração.",
    icon="⚠️",
)

# =========================================================
# MINI TUTORIAL
# =========================================================

with st.expander("📖 Como usar este painel — leia antes de rodar"):
    st.markdown("""
### Passo a passo

| Etapa | Botão | O que faz |
|---|---|---|
| **1** | Extrair dados | Baixa os dados da API do IBGE (localidades) e do SIDRA (PIB) para a pasta `DATA/` |
| **2** | Transformar dados | Limpa, padroniza e cruza os arquivos extraídos na memória |
| **3** | Carregar no Postgres | Insere os dados no banco e exibe um resumo de sanidade |

### Ordem obrigatória
Você **precisa** rodar na ordem: **Extrair → Transformar → Carregar**.  
Pular etapas causa erro. Se mudar o período ou os indicadores, rode tudo do início.

### Quanto tempo demora?
- Série completa (2002–2023, todos os indicadores): **~15–25 min** dependendo da velocidade da API.
- Série curta (2019–2023): **~3–5 min**.
- A barra de progresso mostra o andamento em tempo real.

### Resetar banco
Marque **"Resetar banco antes de carregar"** se quiser recarregar dados do zero.  
Sem essa opção, os dados existentes são mantidos e novos são inseridos/atualizados (upsert).

### Perguntas frequentes

**Por que 2022 e 2023 aparecem com dados incompletos nas análises?**  
O IBGE ainda não publicou o VAB setorial para esses anos na tabela municipal. Apenas o PIB total está disponível. As páginas de Composição Setorial e Evolução Temporal filtram automaticamente até 2021.

**A extração falhou no meio. Posso continuar?**  
Não há resumo automático. Rode novamente — os arquivos já baixados serão sobrescritos, mas o processo é idempotente.

**Posso rodar apenas alguns indicadores?**  
Sim. Desmarque os indicadores no campo "Indicadores" antes de extrair.

**O banco está com dados desatualizados. O que fazer?**  
Marque "Resetar banco", escolha o período desejado e rode as 3 etapas.

**A API do IBGE retornou erro 429 (Too Many Requests).**  
O ETL já aplica um delay de 1s entre chamadas. Se persistir, aguarde alguns minutos e tente novamente.
    """)

st.divider()


# =========================================================
# CONFIGURACAO
# =========================================================

st.subheader("Configuração")

col_esq, col_dir = st.columns([1, 2])

with col_esq:
    periodo_label = st.selectbox(
        "Periodo",
        options=list(PERIODOS.keys()),
        index=1,
    )
    anos = PERIODOS[periodo_label]

    reset = st.checkbox("Resetar banco antes de carregar", value=False)
    if reset:
        st.warning("Todos os dados existentes serão apagados antes da carga.")

with col_dir:
    vars_sel: list[int] = st.multiselect(
        "Indicadores",
        options=list(PRESET_VARS.keys()),
        default=list(PRESET_VARS.keys()),
        format_func=lambda k: PRESET_VARS[k],
    )

if not vars_sel:
    st.error("Selecione ao menos 1 indicador.")
    st.stop()

anos_restritos = [a for a in anos if a >= 2022]
vars_vab_sel   = set(vars_sel) & VARS_ATE_2021
ignorados      = len(vars_vab_sel) * len(anos_restritos)

with st.expander("Resumo da extração", expanded=False):
    c1, c2, c3 = st.columns(3)
    c1.metric("Anos",        len(anos))
    c2.metric("Indicadores", len(vars_sel))
    c3.metric("Ignorados*",  ignorados)

    if ignorados:
        st.caption(
            "* VAB Agropecuária, Indústria, Serviços, Adm. Pública e Impostos "
            "não foram publicados pelo IBGE para 2022 e 2023 na tabela municipal."
        )

    st.write(f"**Período:** {anos[0]} até {anos[-1]}")
    st.write("**Indicadores:** " + ", ".join(PRESET_VARS[v] for v in vars_sel))

st.divider()


# =========================================================
# URL SIDRA
# =========================================================

def sidra_url(ano: int, var_id: int) -> str:
    return (
        f"{SIDRA_BASE}/t/{SIDRA_TABLE}"
        f"/n6/all"
        f"/v/{var_id}"
        f"/p/{ano}"
    )


# =========================================================
# BOTAO 1 — EXTRACAO
# =========================================================

if st.button("Extrair dados", type="primary", use_container_width=True):
    out_dir = str(ROOT / "DATA")

    with st.status("Extraindo localidades (IBGE)...", expanded=False):
        p_regioes = extract_json(
            f"{IBGE_BASE}/regioes",
            headers=HEADERS,
            prefix="ibge_localidades",
            endpoint_label="regioes",
            out_dir=out_dir,
        )
        st.write(f"Regiões: {'✅ OK' if p_regioes.ok else '❌ FALHOU'}")

        p_ufs = extract_json(
            f"{IBGE_BASE}/estados",
            headers=HEADERS,
            prefix="ibge_localidades",
            endpoint_label="ufs",
            out_dir=out_dir,
        )
        st.write(f"UFs: {'✅ OK' if p_ufs.ok else '❌ FALHOU'}")

        p_muns = extract_json(
            f"{IBGE_BASE}/municipios",
            headers=HEADERS,
            prefix="ibge_localidades",
            endpoint_label="municipios",
            out_dir=out_dir,
        )
        st.write(f"Municípios: {'✅ OK' if p_muns.ok else '❌ FALHOU'}")

    total    = len(anos) * len(vars_sel)
    contador = 0
    prog     = st.progress(0, text="Aguarde...")

    sidra_paths: dict[int, dict[int, ExtractResult]] = {a: {} for a in anos}
    erros:   list[dict] = []
    pulados: list[dict] = []

    for ano in anos:
        for var_id in vars_sel:
            if ano >= 2022 and var_id in VARS_ATE_2021:
                pulados.append({"Ano": ano, "Indicador": PRESET_VARS[var_id], "Motivo": "Não publicado pelo IBGE"})
                contador += 1
                prog.progress(int(contador / total * 100), text=f"Pulando {ano} / {PRESET_VARS[var_id]}...")
                continue

            res = extract_json(
                sidra_url(ano, var_id),
                headers=HEADERS,
                prefix="sidra",
                endpoint_label=f"t{SIDRA_TABLE}_ano{ano}_v{var_id}",
                out_dir=out_dir,
                throttle_delay=1.0,
            )

            sidra_paths[ano][var_id] = res

            if not res.ok:
                erros.append({
                    "Ano":       ano,
                    "Indicador": PRESET_VARS.get(var_id, var_id),
                    "Erro":      res.error,
                })

            contador += 1
            prog.progress(
                int(contador / total * 100),
                text=f"{ano} — {PRESET_VARS.get(var_id, var_id)}",
            )

    prog.empty()

    st.session_state["paths"] = {
        "regioes":       p_regioes,
        "ufs":           p_ufs,
        "municipios":    p_muns,
        "sidra_by_year": sidra_paths,
    }

    ok_base = p_regioes.ok and p_ufs.ok and p_muns.ok

    if ok_base and not erros:
        st.success(f"✅ Extração concluída. {len(pulados)} combinações ignoradas (2022/2023 sem VAB).")
    else:
        if not ok_base:
            st.error("❌ Falha ao extrair localidades.")
        if erros:
            st.warning(f"{len(erros)} indicador(es) falharam:")
            st.dataframe(pd.DataFrame(erros), use_container_width=True)

    if pulados:
        with st.expander(f"⚠️ {len(pulados)} combinações ignoradas — 2022/2023 sem VAB setorial"):
            st.dataframe(pd.DataFrame(pulados), use_container_width=True)


# =========================================================
# BOTAO 2 — TRANSFORM
# =========================================================

if st.button("Transformar dados", use_container_width=True):
    paths = st.session_state.get("paths")
    if not paths:
        st.error("Execute a extração primeiro.")
        st.stop()

    with st.spinner("Transformando localidades..."):
        df_regioes    = transform_regioes(paths["regioes"].path)
        df_ufs        = transform_ufs(paths["ufs"].path)
        df_municipios = transform_municipios(paths["municipios"].path)

    dfs_pib:      list[pd.DataFrame] = []
    sidra_by_year = paths["sidra_by_year"]
    total_t       = sum(len(v) for v in sidra_by_year.values())
    cont_t        = 0
    prog_t        = st.progress(0, text="Transformando indicadores...")

    for ano, vars_dict in sidra_by_year.items():
        for var_id, res in vars_dict.items():
            if res.ok and res.path:
                try:
                    df_var = transform_pib_sidra(res.path)
                    if not df_var.empty:
                        dfs_pib.append(df_var)
                except Exception as e:
                    st.warning(f"Erro: ano={ano} var={var_id} — {e}")
            cont_t += 1
            prog_t.progress(
                int(cont_t / total_t * 100),
                text=f"Transformando {ano} / var {var_id}...",
            )

    prog_t.empty()

    if not dfs_pib:
        st.error("Nenhum dado válido para transformar.")
        st.stop()

    df_pib = pd.concat(dfs_pib, ignore_index=True).drop_duplicates(
        subset=["id_municipio", "ano", "codigo_variavel"]
    )

    pack = {
        "df_regioes":    df_regioes,
        "df_ufs":        df_ufs,
        "df_municipios": df_municipios,
        "df_pib":        df_pib,
    }

    st.session_state["pack"] = pack
    st.success("✅ Transform concluído.")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Regiões",    df_regioes.shape[0])
    c2.metric("UFs",        df_ufs.shape[0])
    c3.metric("Municípios", df_municipios.shape[0])
    c4.metric("Linhas PIB", f"{df_pib.shape[0]:,}")


# =========================================================
# BOTAO 3 — LOAD
# =========================================================

if st.button("Carregar no Postgres", use_container_width=True):
    pack = st.session_state.get("pack")
    if not pack:
        st.error("Execute o transform primeiro.")
        st.stop()

    with st.spinner("Carregando no banco..."):
        load_all(engine, pack, reset=reset)

    st.success("✅ Carga concluída.")
    st.dataframe(sanity(engine), use_container_width=True)


# =========================================================
# FOOTER
# =========================================================

st.divider()
st.markdown(
    """
    <div style="text-align:center; opacity:0.55; font-size:0.85rem; padding: 8px 0 4px 0;">
        Desenvolvido por <strong>Danilo Potrich</strong> &nbsp;·&nbsp;
        Dados: <a href="https://sidra.ibge.gov.br/tabela/5938" target="_blank"
            style="color:inherit;">IBGE / SIDRA Tabela 5938</a> &nbsp;·&nbsp;
        Série municipal disponível até <strong>2021</strong> (VAB setorial)
    </div>
    """,
    unsafe_allow_html=True,
)
