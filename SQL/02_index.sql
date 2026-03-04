-- =============================================================
-- ÍNDICES OTIMIZADOS — PIB Brasil
-- Autor  : DBA Optimization
-- Data   : 2026-03-04
-- Propósito: Suportar queries eficientes no dashboard
-- =============================================================

-- =====================================================
-- ÍNDICES NA TABELA FATO
-- =====================================================

-- Remover índices redundantes (se existirem da versão anterior)
DROP INDEX IF EXISTS public.idx_fato_municipio;
DROP INDEX IF EXISTS public.idx_fato_ano_var;

-- Índice composto: (variável, ano, município)
-- Cobre queries que filtram por var + ano e retornam municípios
CREATE INDEX IF NOT EXISTS idx_fato_var_ano_mun
    ON fato_indicador_municipio (id_variavel, ano, id_municipio);

-- Índice composto: (município, ano)
-- Cobre queries de histórico temporal de um município específico
CREATE INDEX IF NOT EXISTS idx_fato_mun_ano
    ON fato_indicador_municipio (id_municipio, ano);

-- =====================================================
-- ÍNDICES NAS DIMENSÕES
-- =====================================================

-- Índice em dim_municipio para joins por UF
CREATE INDEX IF NOT EXISTS idx_municipio_uf
    ON dim_municipio (id_uf);

-- Índice em dim_uf para joins por região
CREATE INDEX IF NOT EXISTS idx_uf_regiao
    ON dim_uf (id_regiao);
