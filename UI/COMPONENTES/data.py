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
    where, params = _where_params_base(flt)

    if flt.get("cidade"):
        where.append("m.nome_municipio ilike :cidade")
        params["cidade"] = f"%{flt['cidade']}%"

    return conn.query(
        f"""
        select r.sigla_regiao, uf.sigla_uf, m.id_municipio,
               m.nome_municipio, f.valor
        from fato_indicador_municipio f
        join dim_municipio m on m.id_municipio = f.id_municipio
        join dim_uf uf       on uf.id_uf = m.id_uf
        join dim_regiao r    on r.id_regiao = uf.id_regiao
        where {' and '.join(where)}
        """,
        params=params, ttl="10m",
    )


def query_pib_uf(conn, flt: dict) -> pd.DataFrame:
    where, params = _where_params_base(flt)

    return conn.query(
        f"""
        select uf.sigla_uf, sum(f.valor) as pib
        from fato_indicador_municipio f
        join dim_municipio m on m.id_municipio = f.id_municipio
        join dim_uf uf       on uf.id_uf = m.id_uf
        join dim_regiao r    on r.id_regiao = uf.id_regiao
        where {' and '.join(where)}
        group by uf.sigla_uf
        order by uf.sigla_uf
        """,
        params=params, ttl="10m",
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
        ttl="10m",
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
    """Composição setorial por UF para um ano — alimenta 03_Composicao."""
    where, params = _where_params_mart(flt)

    return conn.query(
        f"""
        select v.sigla_uf, v.nome_uf, v.sigla_regiao, v.nome_regiao,
               v.pib, v.vab_total, v.vab_agro, v.vab_ind, v.vab_serv, v.vab_apsp,
               v.impostos, v.pct_agro, v.pct_ind, v.pct_serv, v.pct_apsp
        from mart.vw_composicao_uf v
        where {' and '.join(where)}
        order by v.sigla_uf
        """,
        params=params, ttl="10m",
    )


def query_serie_historica(conn, flt: dict) -> pd.DataFrame:
    """Série histórica setorial — alimenta 02_Temporal."""
    params: dict = {}
    where  = [f"v.ano between :ano_ini and :ano_fim"]
    params["ano_ini"] = flt.get("ano_ini", flt["ano"])
    params["ano_fim"] = flt.get("ano_fim", flt["ano"])

    if flt.get("id_regiao") is not None:
        where.append("v.sigla_regiao in (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"v.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select v.ano,
               sum(v.pib)      as pib,
               sum(v.vab_agro) as vab_agro,
               sum(v.vab_ind)  as vab_ind,
               sum(v.vab_serv) as vab_serv,
               sum(v.vab_apsp) as vab_apsp,
               sum(v.impostos) as impostos
        from mart.vw_composicao_uf v
        where {' and '.join(where)}
        group by v.ano
        order by v.ano
        """,
        params=params, ttl="10m",
    )


def query_concentracao_uf(conn, flt: dict) -> pd.DataFrame:
    """Métricas de concentração por UF — alimenta 05_Concentracao."""
    where, params = _where_params_base(flt)

    return conn.query(
        f"""
        with base as (
            select uf.sigla_uf, m.id_municipio, f.valor
            from fato_indicador_municipio f
            join dim_municipio m on m.id_municipio = f.id_municipio
            join dim_uf uf       on uf.id_uf = m.id_uf
            join dim_regiao r    on r.id_regiao = uf.id_regiao
            where {' and '.join(where)}
        ),
        tot as (
            select sigla_uf, sum(valor) as total from base group by sigla_uf
        ),
        ranked as (
            select b.*, t.total,
                   row_number() over (partition by b.sigla_uf order by b.valor desc nulls last) as rn
            from base b join tot t on t.sigla_uf = b.sigla_uf
        )
        select
            sigla_uf,
            count(*)                                                                   as n_municipios,
            max(case when rn = 1 then valor / nullif(total, 0) end)                   as top1_share,
            sum(case when rn <= 10 then valor / nullif(total, 0) else 0 end)          as top10_share,
            sum((valor / nullif(total,0)) * (valor / nullif(total,0)))                as hhi
        from ranked
        group by sigla_uf
        order by top1_share desc
        """,
        params=params, ttl="10m",
    )
