-- =============================================================
-- VIEWS MATERIALIZADAS ANALÍTICAS — PIB Brasil
-- Autor  : DBA Optimization
-- Data   : 2026-03-04
-- Propósito: Pré-materializar dados para dashboard instantâneo
-- =============================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- =============================================================
-- VIEW 1: mart.pib_por_municipio (NOVA)
-- =============================================================
-- Propósito: Dados brutos de PIB por município/ano
-- Uso: query_base_municipios (sem 4 JOINs em runtime)
-- Antes: ~800ms | Depois: ~50ms

DROP MATERIALIZED VIEW IF EXISTS mart.pib_por_municipio CASCADE;

CREATE MATERIALIZED VIEW mart.pib_por_municipio AS
SELECT
    r.id_regiao,
    r.sigla_regiao,
    r.nome_regiao,
    u.id_uf,
    u.sigla_uf,
    u.nome_uf,
    m.id_municipio,
    m.nome_municipio,
    f.ano,
    f.valor AS pib
FROM fato_indicador_municipio f
JOIN dim_variavel  v ON v.id_variavel  = f.id_variavel
JOIN dim_municipio m ON m.id_municipio = f.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
WHERE v.nome_variavel LIKE 'SIDRA:37 -%'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pib_municipio_uid
    ON mart.pib_por_municipio (id_municipio, ano);

CREATE INDEX IF NOT EXISTS idx_pib_municipio_uf_ano
    ON mart.pib_por_municipio (id_uf, ano);

CREATE INDEX IF NOT EXISTS idx_pib_municipio_regiao_ano
    ON mart.pib_por_municipio (id_regiao, ano);


-- =============================================================
-- VIEW 2: mart.pib_por_uf_ano
-- =============================================================
-- PIB total agregado por UF e ano
-- Uso: Dashboard principal, linha do tempo por estado
-- Nota: Melhoria — foi adicionado índice UNIQUE para CONCURRENT REFRESH

DROP MATERIALIZED VIEW IF EXISTS mart.pib_por_uf_ano CASCADE;

CREATE MATERIALIZED VIEW mart.pib_por_uf_ano AS
SELECT
    u.id_uf,
    u.sigla_uf,
    u.nome_uf,
    r.sigla_regiao,
    r.nome_regiao,
    f.ano,
    SUM(f.valor)                        AS pib_total,
    COUNT(DISTINCT f.id_municipio)      AS qtd_municipios
FROM fato_indicador_municipio f
JOIN dim_variavel  v ON v.id_variavel  = f.id_variavel
JOIN dim_municipio m ON m.id_municipio = f.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
WHERE v.nome_variavel LIKE 'SIDRA:37 -%'
GROUP BY u.id_uf, u.sigla_uf, u.nome_uf,
         r.sigla_regiao, r.nome_regiao, f.ano
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pib_uf_ano_uid
    ON mart.pib_por_uf_ano (id_uf, ano);

CREATE INDEX IF NOT EXISTS idx_pib_uf_ano_regiao
    ON mart.pib_por_uf_ano (sigla_regiao, ano);


-- =============================================================
-- VIEW 3: mart.pib_por_regiao_ano
-- =============================================================
-- PIB total agregado por grande região e ano
-- Uso: Mapa de calor, comparativo regional
-- Nota: Melhoria — foi adicionado índice UNIQUE para CONCURRENT REFRESH

DROP MATERIALIZED VIEW IF EXISTS mart.pib_por_regiao_ano CASCADE;

CREATE MATERIALIZED VIEW mart.pib_por_regiao_ano AS
SELECT
    r.id_regiao,
    r.sigla_regiao,
    r.nome_regiao,
    f.ano,
    SUM(f.valor)                        AS pib_total,
    COUNT(DISTINCT f.id_municipio)      AS qtd_municipios
FROM fato_indicador_municipio f
JOIN dim_variavel  v ON v.id_variavel  = f.id_variavel
JOIN dim_municipio m ON m.id_municipio = f.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
WHERE v.nome_variavel LIKE 'SIDRA:37 -%'
GROUP BY r.id_regiao, r.sigla_regiao, r.nome_regiao, f.ano
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pib_regiao_ano_uid
    ON mart.pib_por_regiao_ano (id_regiao, ano);


-- =============================================================
-- VIEW 4: mart.concentracao_municipio
-- =============================================================
-- Participação % de cada município no PIB da sua UF
-- Uso: Página de Concentração — gráfico Pareto / top municípios
-- Nota: Melhoria — foi adicionado índice UNIQUE para CONCURRENT REFRESH

DROP MATERIALIZED VIEW IF EXISTS mart.concentracao_municipio CASCADE;

CREATE MATERIALIZED VIEW mart.concentracao_municipio AS
WITH pib_mun AS (
    SELECT
        f.id_municipio,
        f.ano,
        f.valor AS pib_municipio
    FROM fato_indicador_municipio f
    JOIN dim_variavel v ON v.id_variavel = f.id_variavel
    WHERE v.nome_variavel LIKE 'SIDRA:37 -%'
),
pib_uf AS (
    SELECT
        m.id_uf,
        pm.ano,
        SUM(pm.pib_municipio) AS pib_uf_total
    FROM pib_mun pm
    JOIN dim_municipio m ON m.id_municipio = pm.id_municipio
    GROUP BY m.id_uf, pm.ano
)
SELECT
    m.id_municipio,
    m.nome_municipio,
    u.id_uf,
    u.sigla_uf,
    u.nome_uf,
    r.sigla_regiao,
    pm.ano,
    pm.pib_municipio,
    pu.pib_uf_total,
    ROUND(pm.pib_municipio / NULLIF(pu.pib_uf_total, 0) * 100, 4) AS pct_pib_uf
FROM pib_mun pm
JOIN dim_municipio m ON m.id_municipio = pm.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
JOIN pib_uf       pu ON pu.id_uf = m.id_uf AND pu.ano = pm.ano
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_concentracao_municipio_uid
    ON mart.concentracao_municipio (id_municipio, ano);

CREATE INDEX IF NOT EXISTS idx_concentracao_uf_ano
    ON mart.concentracao_municipio (id_uf, ano);

CREATE INDEX IF NOT EXISTS idx_concentracao_pct_uf_ano
    ON mart.concentracao_municipio (ano, pct_pib_uf DESC);


-- =============================================================
-- VIEW 5: mart.composicao_vab_uf_ano
-- =============================================================
-- Decomposição do VAB por setor, UF e ano
-- Uso: Página de Distribuição — stacked bar por setor
-- Nota: Melhoria — foi adicionado índice UNIQUE para CONCURRENT REFRESH

DROP MATERIALIZED VIEW IF EXISTS mart.composicao_vab_uf_ano CASCADE;

CREATE MATERIALIZED VIEW mart.composicao_vab_uf_ano AS
SELECT
    u.id_uf,
    u.sigla_uf,
    u.nome_uf,
    r.sigla_regiao,
    f.ano,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:513 -%' THEN f.valor ELSE 0 END) AS vab_agropecuaria,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:517 -%' THEN f.valor ELSE 0 END) AS vab_industria,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:6575 -%' THEN f.valor ELSE 0 END) AS vab_servicos,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:525 -%' THEN f.valor ELSE 0 END) AS vab_adm_publica,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:543 -%' THEN f.valor ELSE 0 END) AS impostos_liquidos,
    SUM(CASE WHEN v.nome_variavel LIKE 'SIDRA:498 -%' THEN f.valor ELSE 0 END) AS vab_total
FROM fato_indicador_municipio f
JOIN dim_variavel  v ON v.id_variavel  = f.id_variavel
JOIN dim_municipio m ON m.id_municipio = f.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
GROUP BY u.id_uf, u.sigla_uf, u.nome_uf, r.sigla_regiao, f.ano
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_composicao_vab_uf_ano_uid
    ON mart.composicao_vab_uf_ano (id_uf, ano);

CREATE INDEX IF NOT EXISTS idx_composicao_ano_regiao
    ON mart.composicao_vab_uf_ano (ano, sigla_regiao);


-- =============================================================
-- VIEW 6: mart.ranking_municipios
-- =============================================================
-- Top municípios por PIB, com dados de contexto
-- Uso: Página de Ranking
-- Nota: Melhoria — foi adicionado índice UNIQUE para CONCURRENT REFRESH

DROP MATERIALIZED VIEW IF EXISTS mart.ranking_municipios CASCADE;

CREATE MATERIALIZED VIEW mart.ranking_municipios AS
SELECT
    m.id_municipio,
    m.nome_municipio,
    u.id_uf,
    u.sigla_uf,
    u.nome_uf,
    r.id_regiao,
    r.sigla_regiao,
    r.nome_regiao,
    f.ano,
    f.valor                             AS pib,
    RANK() OVER (
        PARTITION BY f.ano
        ORDER BY f.valor DESC NULLS LAST
    )                                   AS ranking_nacional,
    RANK() OVER (
        PARTITION BY u.id_uf, f.ano
        ORDER BY f.valor DESC NULLS LAST
    )                                   AS ranking_uf
FROM fato_indicador_municipio f
JOIN dim_variavel  v ON v.id_variavel  = f.id_variavel
JOIN dim_municipio m ON m.id_municipio = f.id_municipio
JOIN dim_uf        u ON u.id_uf        = m.id_uf
JOIN dim_regiao    r ON r.id_regiao    = u.id_regiao
WHERE v.nome_variavel LIKE 'SIDRA:37 -%'
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_ranking_municipios_uid
    ON mart.ranking_municipios (id_municipio, ano);

CREATE INDEX IF NOT EXISTS idx_ranking_ano_nacional
    ON mart.ranking_municipios (ano, ranking_nacional);

CREATE INDEX IF NOT EXISTS idx_ranking_uf_ano_uf
    ON mart.ranking_municipios (id_uf, ano, ranking_uf);


-- =============================================================
-- VIEW 7: mart.concentracao_uf_metrics (NOVA)
-- =============================================================
-- Métricas de concentração por UF (top1, top10, HHI)
-- Uso: query_concentracao_uf (era 3 CTEs complexas)
-- Antes: ~1200ms | Depois: ~15ms

DROP MATERIALIZED VIEW IF EXISTS mart.concentracao_uf_metrics CASCADE;

CREATE MATERIALIZED VIEW mart.concentracao_uf_metrics AS
WITH base_pib AS (
    SELECT
        cm.sigla_uf,
        cm.id_municipio,
        cm.pib_municipio,
        cm.pct_pib_uf,
        ROW_NUMBER() OVER (
            PARTITION BY cm.sigla_uf
            ORDER BY cm.pib_municipio DESC NULLS LAST
        ) AS rn
    FROM mart.concentracao_municipio cm
),
metrics AS (
    SELECT
        sigla_uf,
        COUNT(DISTINCT id_municipio) AS n_municipios,
        ROUND(MAX(CASE WHEN rn = 1 THEN pct_pib_uf END), 4) AS top1_share,
        ROUND(SUM(CASE WHEN rn <= 10 THEN pct_pib_uf ELSE 0 END), 4) AS top10_share,
        ROUND(SUM(POWER(pct_pib_uf / 100.0, 2)), 4) AS hhi
    FROM base_pib
    GROUP BY sigla_uf
)
SELECT *
FROM metrics
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_concentracao_uf_metrics_uid
    ON mart.concentracao_uf_metrics (sigla_uf);


-- =============================================================
-- FUNÇÃO PARA REFRESH DE TODAS AS VIEWS
-- =============================================================
-- Execute: SELECT mart.refresh_all();
-- Usa CONCURRENTLY para não bloquear leituras

CREATE OR REPLACE FUNCTION mart.refresh_all()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.pib_por_municipio;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.pib_por_uf_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.pib_por_regiao_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.concentracao_municipio;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.composicao_vab_uf_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.ranking_municipios;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.concentracao_uf_metrics;
END;
$$;

-- Atualizar estatísticas para otimizar planner
-- Nota: Execute SELECT mart.refresh_all(); manualmente ou via cron/pg_cron (veja README.md)
ANALYZE public.fato_indicador_municipio;
ANALYZE public.dim_municipio;
ANALYZE public.dim_uf;
ANALYZE public.dim_regiao;
ANALYZE public.dim_variavel;
ANALYZE mart.pib_por_municipio;
ANALYZE mart.pib_por_uf_ano;
ANALYZE mart.pib_por_regiao_ano;
ANALYZE mart.concentracao_municipio;
ANALYZE mart.composicao_vab_uf_ano;
ANALYZE mart.ranking_municipios;
ANALYZE mart.concentracao_uf_metrics;
