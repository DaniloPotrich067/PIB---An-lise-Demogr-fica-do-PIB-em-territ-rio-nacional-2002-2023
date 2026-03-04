from __future__ import annotations

import pandas as pd
from COMPONENTES.shared import sql_in_params


def _where_params_base(flt: dict):
    where  = ["f.ano = :ano", "f.id_variavel = :id_variavel"]
    params = {"ano": flt["ano"], "id_variavel": flt["id_variavel"]}

    if flt.get("id_regiao") is not None:
        where.append("r.id_regiao = :id_regiao")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"uf.sigla_uf in ({ph})")

    return where, params


def _where_params_mart(flt: dict, uf_col: str = "v.sigla_uf", reg_col: str = "v.sigla_regiao"):
    """WHERE + params para queries diretas nas views mart (sem join)."""
    where  = ["v.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append(f"{reg_col} in (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"{uf_col} in ({ph})")

    return where, params


# ── Queries originais ─────────────────────────────────────────────────────────

def query_base_municipios(conn, flt: dict) -> pd.DataFrame:
    """Busca bruta de PIB por município — usa view mart.pib_por_municipio (PRÉ-MATERIALIZADA).
    
    ANTES: 4 JOINs + filtros em runtime → LENTO
    DEPOIS: SELECT na view pré-processada → RÁPIDO (+ índices)
    """
    where  = ["pib.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append("pib.id_regiao = :id_regiao")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"pib.sigla_uf in ({ph})")

    if flt.get("cidade"):
        where.append("pib.nome_municipio ilike :cidade")
        params["cidade"] = f"%{flt['cidade']}%"

    return conn.query(
        f"""
        select pib.sigla_regiao, pib.sigla_uf, pib.id_municipio,
               pib.nome_municipio, pib.pib as valor
        from mart.pib_por_municipio pib
        where {' and '.join(where)}
        order by pib.pib desc
        """,
        params=params, ttl="1h",
    )


def query_pib_uf(conn, flt: dict) -> pd.DataFrame:
    """PIB agregado por UF — usa view mart.pib_por_uf_ano (JÁ AGREGADA).
    
    ANTES: GROUP BY em runtime → LENTO
    DEPOIS: Leitura direta de view materializada → RÁPIDO
    """
    where  = ["pib_uf.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append("pib_uf.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"pib_uf.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select pib_uf.sigla_uf, pib_uf.pib_total as pib
        from mart.pib_por_uf_ano pib_uf
        where {' and '.join(where)}
        order by pib_uf.sigla_uf
        """,
        params=params, ttl="1h",
    )


def query_sanity_counts(conn) -> pd.DataFrame:
    return conn.query(
        """
        select
          (select count(*) from dim_regiao)              as n_regiao,
          (select count(*) from dim_uf)                  as n_uf,
          (select count(*) from dim_municipio)           as n_municipio,
          (select count(*) from dim_variavel)            as n_variavel,
          (select count(*) from fato_indicador_municipio) as n_fato
        """,
        ttl="1h",
    )


def query_total_municipios(conn) -> int:
    return int(conn.query(
        "select count(*) as n from dim_municipio;", ttl="1h"
    )["n"].iloc[0])


def query_missing_municipios_por_uf(conn, flt: dict) -> pd.DataFrame:
    params = {"ano": flt["ano"], "id_variavel": flt["id_variavel"]}
    extra  = ""

    if flt.get("id_regiao") is not None:
        extra += " and r.id_regiao = :id_regiao"
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("muf", flt["ufs"], params)
        extra += f" and uf.sigla_uf in ({ph})"

    return conn.query(
        f"""
        select uf.sigla_uf, count(*) as municipios_sem_pib
        from dim_municipio m
        join dim_uf uf    on uf.id_uf = m.id_uf
        join dim_regiao r on r.id_regiao = uf.id_regiao
        left join fato_indicador_municipio f
            on f.id_municipio = m.id_municipio
           and f.ano = :ano
           and f.id_variavel = :id_variavel
        where f.id_municipio is null {extra}
        group by uf.sigla_uf
        order by municipios_sem_pib desc
        """,
        params=params, ttl="10m",
    )


# ── Queries novas (mart views) ────────────────────────────────────────────────

def query_composicao_uf(conn, flt: dict) -> pd.DataFrame:
    """Composição setorial por UF para um ano — usa view mart.composicao_vab_uf_ano.
    
    View já traz VAB_AGRO, VAB_IND, VAB_SERV, VAB_ADM_PUBLICA, IMPOSTOS pré-calculados.
    """
    where  = ["comp.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append("comp.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"comp.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select comp.sigla_uf, comp.nome_uf, comp.sigla_regiao, comp.nome_regiao,
               comp.vab_agropecuaria as vab_agro,
               comp.vab_industria as vab_ind,
               comp.vab_servicos as vab_serv,
               comp.vab_adm_publica as vab_apsp,
               comp.impostos_liquidos as impostos,
               (comp.vab_agropecuaria / NULLIF(comp.vab_total, 0)) * 100 as pct_agro,
               (comp.vab_industria / NULLIF(comp.vab_total, 0)) * 100 as pct_ind,
               (comp.vab_servicos / NULLIF(comp.vab_total, 0)) * 100 as pct_serv,
               (comp.vab_adm_publica / NULLIF(comp.vab_total, 0)) * 100 as pct_apsp,
               comp.vab_total as pib
        from mart.composicao_vab_uf_ano comp
        where {' and '.join(where)}
        order by comp.sigla_uf
        """,
        params=params, ttl="10m",
    )


def query_serie_historica(conn, flt: dict) -> pd.DataFrame:
    """Série histórica setorial — alimenta 02_Temporal.
    
    agregação de ano_ini até ano_fim usando view materializada.
    """
    params: dict = {}
    where  = [f"comp.ano between :ano_ini and :ano_fim"]
    params["ano_ini"] = flt.get("ano_ini", flt["ano"])
    params["ano_fim"] = flt.get("ano_fim", flt["ano"])

    if flt.get("id_regiao") is not None:
        where.append("comp.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"comp.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select comp.ano,
               sum(comp.vab_agropecuaria) as vab_agro,
               sum(comp.vab_industria) as vab_ind,
               sum(comp.vab_servicos) as vab_serv,
               sum(comp.vab_adm_publica) as vab_apsp,
               sum(comp.impostos_liquidos) as impostos,
               sum(comp.vab_total) as pib
        from mart.composicao_vab_uf_ano comp
        where {' and '.join(where)}
        group by comp.ano
        order by comp.ano
        """,
        params=params, ttl="10m",
    )


def query_concentracao_uf(conn, flt: dict) -> pd.DataFrame:
    """Métricas de concentração por UF — usa view mart.concentracao_uf_metrics (JÁ CALCULADA).
    
    ANTES: CTEs + ROW_NUMBER em runtime → MUITO LENTO
    DEPOIS: SELECT na view pré-calculada → MUITO RÁPIDO
    """
    where  = ["true"]
    params = {}

    if flt.get("id_regiao") is not None:
        where.append("m.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"m.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select m.sigla_uf,
               m.n_municipios,
               m.top1_share,
               m.top10_share,
               m.hhi
        from mart.concentracao_uf_metrics m
        where {' and '.join(where)}
        order by m.top1_share desc
        """,
        params=params, ttl="10m",
    )
