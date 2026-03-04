-- =============================================================
-- DIMENSÕES
-- =============================================================

CREATE TABLE IF NOT EXISTS dim_regiao (
    id_regiao    INT  PRIMARY KEY,
    sigla_regiao TEXT NOT NULL,
    nome_regiao  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_uf (
    id_uf     INT  PRIMARY KEY,
    sigla_uf  TEXT NOT NULL,
    nome_uf   TEXT NOT NULL,
    id_regiao INT  NOT NULL REFERENCES dim_regiao(id_regiao)
);

CREATE TABLE IF NOT EXISTS dim_municipio (
    id_municipio   INT  PRIMARY KEY,
    nome_municipio TEXT NOT NULL,
    id_uf          INT  NOT NULL REFERENCES dim_uf(id_uf)
);

-- nome_variavel é a chave natural usada pelo load.py (UNIQUE obrigatório)
CREATE TABLE IF NOT EXISTS dim_variavel (
    id_variavel   SERIAL PRIMARY KEY,
    nome_variavel TEXT   NOT NULL UNIQUE,
    unidade       TEXT
);

-- =============================================================
-- FATO
-- PK composta garante idempotência nos upserts do load.py
-- =============================================================

CREATE TABLE IF NOT EXISTS fato_indicador_municipio (
    id_municipio INT     NOT NULL REFERENCES dim_municipio(id_municipio),
    id_variavel  INT     NOT NULL REFERENCES dim_variavel(id_variavel),
    ano          INT     NOT NULL,
    valor        NUMERIC,
    PRIMARY KEY (id_municipio, id_variavel, ano)
);

-- =============================================================
-- ÍNDICES — performance de dashboard
-- Nota: Índices otimizados agora estão em 02_index.sql
-- =============================================================

-- Índices básicos (provisionados aqui, refinados em 02_index.sql)
CREATE INDEX IF NOT EXISTS idx_fato_ano_var
    ON fato_indicador_municipio (ano, id_variavel);

CREATE INDEX IF NOT EXISTS idx_fato_municipio
    ON fato_indicador_municipio (id_municipio);

CREATE INDEX IF NOT EXISTS idx_municipio_uf
    ON dim_municipio (id_uf);

CREATE INDEX IF NOT EXISTS idx_uf_regiao
    ON dim_uf (id_regiao);

-- =============================================================
-- SCHEMA mart — views materializadas analíticas
-- Nota: Views agora estão em 03_materialized_views.sql
-- Execute a sequência: 00_Reset → 01_query → 02_index → 03_materialized_views → 04_tests
-- =============================================================

