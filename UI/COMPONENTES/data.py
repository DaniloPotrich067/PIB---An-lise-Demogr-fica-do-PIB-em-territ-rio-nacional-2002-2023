from __future__ import annotations

import pandas as pd
from COMPONENTES.shared import sql_in_params


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS INTERNOS
# ─────────────────────────────────────────────────────────────────────────────

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
    where  = ["v.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append(
            f"{reg_col} in (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)"
        )
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"{uf_col} in ({ph})")

    return where, params


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES — fato (respeitam id_variavel — ttl=0 para atualizar com filtro)
# ─────────────────────────────────────────────────────────────────────────────

def query_base_municipios(conn, flt: dict) -> pd.DataFrame:
    """Valor por município direto da fato — respeita id_variavel."""
    where  = ["f.ano = :ano", "f.id_variavel = :id_variavel"]
    params = {"ano": flt["ano"], "id_variavel": flt["id_variavel"]}

    if flt.get("id_regiao") is not None:
        where.append("u.id_regiao = :id_regiao")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"u.sigla_uf in ({ph})")

    if flt.get("cidade"):
        where.append("m.nome_municipio ilike :cidade")
        params["cidade"] = f"%{flt['cidade']}%"

    return conn.query(
        f"""
        select r.sigla_regiao, u.sigla_uf, m.id_municipio,
               m.nome_municipio, f.valor
        from fato_indicador_municipio f
        join dim_municipio m on m.id_municipio = f.id_municipio
        join dim_uf        u on u.id_uf        = m.id_uf
        join dim_regiao    r on r.id_regiao     = u.id_regiao
        where {" and ".join(where)}
        order by f.valor desc
        """,
        params=params, ttl=0,
    )


def query_valor_por_uf(conn, flt: dict) -> pd.DataFrame:
    """Agrega valor por UF respeitando id_variavel — mapa dinâmico."""
    where  = ["f.ano = :ano", "f.id_variavel = :id_variavel"]
    params = {"ano": flt["ano"], "id_variavel": flt["id_variavel"]}

    if flt.get("id_regiao") is not None:
        where.append("u.id_regiao = :id_regiao")
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"u.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select u.sigla_uf, sum(f.valor) as pib
        from fato_indicador_municipio f
        join dim_municipio m on m.id_municipio = f.id_municipio
        join dim_uf        u on u.id_uf        = m.id_uf
        where {" and ".join(where)}
        group by u.sigla_uf
        order by u.sigla_uf
        """,
        params=params, ttl=0,
    )


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
        join dim_uf uf    on uf.id_uf     = m.id_uf
        join dim_regiao r on r.id_regiao  = uf.id_regiao
        left join fato_indicador_municipio f
            on f.id_municipio = m.id_municipio
           and f.ano          = :ano
           and f.id_variavel  = :id_variavel
        where f.id_municipio is null {extra}
        group by uf.sigla_uf
        order by municipios_sem_pib desc
        """,
        params=params, ttl=0,
    )


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES — mart views (fixas, sem id_variavel — ttl longo OK)
# ─────────────────────────────────────────────────────────────────────────────

def query_pib_uf(conn, flt: dict) -> pd.DataFrame:
    """PIB agregado por UF — mart.pib_por_uf_ano. Usado onde indicador é fixo."""
    where  = ["pib_uf.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append(
            "pib_uf.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)"
        )
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"pib_uf.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select pib_uf.sigla_uf, pib_uf.pib_total as pib
        from mart.pib_por_uf_ano pib_uf
        where {" and ".join(where)}
        order by pib_uf.sigla_uf
        """,
        params=params, ttl="1h",
    )


def query_composicao_uf(conn, flt: dict) -> pd.DataFrame:
    where  = ["comp.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append(
            "comp.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)"
        )
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"comp.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select comp.sigla_uf, comp.sigla_regiao,
               u.nome_uf,
               r.nome_regiao,
               comp.vab_agropecuaria                                    as vab_agro,
               comp.vab_industria                                       as vab_ind,
               comp.vab_servicos                                        as vab_serv,
               comp.vab_adm_publica                                     as vab_apsp,
               comp.impostos_liquidos                                   as impostos,
               (comp.vab_agropecuaria / NULLIF(comp.vab_total, 0))*100  as pct_agro,
               (comp.vab_industria    / NULLIF(comp.vab_total, 0))*100  as pct_ind,
               (comp.vab_servicos     / NULLIF(comp.vab_total, 0))*100  as pct_serv,
               (comp.vab_adm_publica  / NULLIF(comp.vab_total, 0))*100  as pct_apsp,
               comp.vab_total                                           as pib
        from mart.composicao_vab_uf_ano comp
        join dim_uf     u on u.sigla_uf     = comp.sigla_uf
        join dim_regiao r on r.sigla_regiao = comp.sigla_regiao
        where {" and ".join(where)}
        order by comp.sigla_uf
        """,
        params=params, ttl="10m",
    )


def query_serie_historica(conn, flt: dict) -> pd.DataFrame:
    params = {
        "ano_ini": flt.get("ano_ini", flt["ano"]),
        "ano_fim": flt.get("ano_fim", flt["ano"]),
    }
    where = ["comp.ano between :ano_ini and :ano_fim"]

    if flt.get("id_regiao") is not None:
        where.append(
            "comp.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)"
        )
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"comp.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select comp.ano,
               sum(comp.vab_agropecuaria)  as vab_agro,
               sum(comp.vab_industria)     as vab_ind,
               sum(comp.vab_servicos)      as vab_serv,
               sum(comp.vab_adm_publica)   as vab_apsp,
               sum(comp.impostos_liquidos) as impostos,
               sum(comp.vab_total)         as pib
        from mart.composicao_vab_uf_ano comp
        where {" and ".join(where)}
        group by comp.ano
        order by comp.ano
        """,
        params=params, ttl="10m",
    )


def query_concentracao_uf(conn, flt: dict) -> pd.DataFrame:
    where  = ["m.ano = :ano"]
    params = {"ano": flt["ano"]}

    if flt.get("id_regiao") is not None:
        where.append(
            "m.sigla_regiao = (select sigla_regiao from dim_regiao where id_regiao = :id_regiao)"
        )
        params["id_regiao"] = flt["id_regiao"]

    if flt.get("ufs"):
        ph = sql_in_params("uf", flt["ufs"], params)
        where.append(f"m.sigla_uf in ({ph})")

    return conn.query(
        f"""
        select m.sigla_uf,
               m.n_municipios,
               m.top1_pct,
               m.top10_pct,
               m.hhi
        from mart.concentracao_uf_metrics m
        where {" and ".join(where)}
        order by m.top1_pct desc
        """,
        params=params, ttl="10m",
    )


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES — utilitárias
# ─────────────────────────────────────────────────────────────────────────────

def query_anos_disponiveis(conn) -> list[int]:
    result = conn.query(
        "SELECT DISTINCT ano FROM fato_indicador_municipio ORDER BY ano DESC",
        ttl="1h",
    )
    return result["ano"].tolist() if not result.empty else []


def query_sanity_counts(conn) -> pd.DataFrame:
    return conn.query(
        """
        select
            (select count(*) from dim_regiao)               as n_regiao,
            (select count(*) from dim_uf)                   as n_uf,
            (select count(*) from dim_municipio)            as n_municipio,
            (select count(*) from dim_variavel)             as n_variavel,
            (select count(*) from fato_indicador_municipio) as n_fato
        """,
        ttl="1h",
    )


def query_total_municipios(conn) -> int:
    return int(
        conn.query("select count(*) as n from dim_municipio;", ttl="1h")["n"].iloc[0]
    )
