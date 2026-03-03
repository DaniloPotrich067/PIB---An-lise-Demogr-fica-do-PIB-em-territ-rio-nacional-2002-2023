-- =============================================================
-- DIMENSÕES
-- =============================================================

CREATE TABLE dim_regiao (
    id_regiao    INT  PRIMARY KEY,
    sigla_regiao TEXT NOT NULL,
    nome_regiao  TEXT NOT NULL
);

CREATE TABLE dim_uf (
    id_uf     INT  PRIMARY KEY,
    sigla_uf  TEXT NOT NULL,
    nome_uf   TEXT NOT NULL,
    id_regiao INT  NOT NULL REFERENCES dim_regiao(id_regiao)
);

CREATE TABLE dim_municipio (
    id_municipio   INT  PRIMARY KEY,
    nome_municipio TEXT NOT NULL,
    id_uf          INT  NOT NULL REFERENCES dim_uf(id_uf)
);

-- nome_variavel é a chave natural usada pelo load.py (UNIQUE obrigatório)
CREATE TABLE dim_variavel (
    id_variavel   SERIAL PRIMARY KEY,
    nome_variavel TEXT   NOT NULL UNIQUE,
    unidade       TEXT
);

-- =============================================================
-- FATO
-- PK composta garante idempotência nos upserts do load.py
-- =============================================================

CREATE TABLE fato_indicador_municipio (
    id_municipio INT     NOT NULL REFERENCES dim_municipio(id_municipio),
    id_variavel  INT     NOT NULL REFERENCES dim_variavel(id_variavel),
    ano          INT     NOT NULL,
    valor        NUMERIC,
    PRIMARY KEY (id_municipio, id_variavel, ano)
);

-- =============================================================
-- ÍNDICES — performance de dashboard
-- =============================================================

-- Filtro por ano + variável (consulta mais comum)
CREATE INDEX idx_fato_ano_var
    ON fato_indicador_municipio (ano, id_variavel);

-- Histórico de um município específico
CREATE INDEX idx_fato_municipio
    ON fato_indicador_municipio (id_municipio);

-- Consultas por UF (joins frequentes nos dashboards)
CREATE INDEX idx_municipio_uf
    ON dim_municipio (id_uf);

-- Consultas por região
CREATE INDEX idx_uf_regiao
    ON dim_uf (id_regiao);

-- =============================================================
-- SCHEMA mart — views materializadas analíticas
-- =============================================================

CREATE SCHEMA IF NOT EXISTS mart;

-- -------------------------------------------------------------
-- mart.pib_por_uf_ano
-- PIB total agregado por UF e ano
-- Uso: Dashboard principal, linha do tempo por estado
-- -------------------------------------------------------------
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
WHERE v.nome_variavel LIKE 'SIDRA:37 -%'   -- PIB a preços correntes
GROUP BY u.id_uf, u.sigla_uf, u.nome_uf,
         r.sigla_regiao, r.nome_regiao, f.ano
WITH DATA;

CREATE UNIQUE INDEX ON mart.pib_por_uf_ano (id_uf, ano);


-- -------------------------------------------------------------
-- mart.pib_por_regiao_ano
-- PIB total agregado por grande região e ano
-- Uso: Mapa de calor, comparativo regional
-- -------------------------------------------------------------
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

CREATE UNIQUE INDEX ON mart.pib_por_regiao_ano (id_regiao, ano);


-- -------------------------------------------------------------
-- mart.concentracao_municipio
-- Participação % de cada município no PIB da sua UF (último ano)
-- Uso: Página de Concentração — gráfico Pareto / top municípios
-- -------------------------------------------------------------
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

CREATE INDEX ON mart.concentracao_municipio (id_uf, ano);
CREATE INDEX ON mart.concentracao_municipio (ano, pct_pib_uf DESC);


-- -------------------------------------------------------------
-- mart.composicao_vab_uf_ano
-- Decomposição do VAB por setor, UF e ano
-- Uso: Página de Distribuição — stacked bar por setor
-- -------------------------------------------------------------
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

CREATE UNIQUE INDEX ON mart.composicao_vab_uf_ano (id_uf, ano);


-- -------------------------------------------------------------
-- mart.ranking_municipios
-- Top municípios por PIB, com dados de contexto
-- Uso: Página de Ranking
-- -------------------------------------------------------------
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

CREATE INDEX ON mart.ranking_municipios (ano, ranking_nacional);
CREATE INDEX ON mart.ranking_municipios (id_uf, ano, ranking_uf);


-- =============================================================
-- FUNÇÃO para atualizar todas as views de uma vez
-- Use após cada carga: SELECT mart.refresh_all();
-- =============================================================

CREATE OR REPLACE FUNCTION mart.refresh_all()
RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.pib_por_uf_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.pib_por_regiao_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.concentracao_municipio;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.composicao_vab_uf_ano;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mart.ranking_municipios;
END;
$$;
