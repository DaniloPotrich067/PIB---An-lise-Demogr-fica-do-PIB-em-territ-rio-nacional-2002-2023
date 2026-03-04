-- ================================================================
-- 02_testes.sql | PIB Brasil — Suite de Validação Completa
-- Autor  : Danilo Potrich
-- Banco  : PIB_Brasil
-- Data   : 2026-03-03
-- ================================================================

-- ================================================================
-- SEÇÃO 1 — CONTAGEM DAS DIMENSÕES
-- ================================================================

-- [T01] Regiões (ESPERADO: 5)
SELECT 'dim_regiao' AS tabela, COUNT(*) AS total,
    CASE WHEN COUNT(*) = 5 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_regiao;

-- [T02] UFs (ESPERADO: 27)
SELECT 'dim_uf' AS tabela, COUNT(*) AS total,
    CASE WHEN COUNT(*) = 27 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_uf;

-- [T03] Municípios (ESPERADO: entre 5560 e 5572)
SELECT 'dim_municipio' AS tabela, COUNT(*) AS total,
    CASE WHEN COUNT(*) BETWEEN 5560 AND 5572 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_municipio;

-- [T04] Variáveis (ESPERADO: >= 5)
SELECT 'dim_variavel' AS tabela, COUNT(*) AS total,
    CASE WHEN COUNT(*) >= 5 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_variavel;

-- [T04-B] Lista de variáveis (conferência visual)
SELECT id_variavel, nome_variavel, unidade
FROM dim_variavel ORDER BY id_variavel;


-- ================================================================
-- SEÇÃO 2 — INTEGRIDADE REFERENCIAL
-- ================================================================
-- Todos devem retornar total_orfaos = 0

-- [T05] UFs sem região (ESPERADO: 0)
SELECT 'UFs sem região' AS teste, COUNT(*) AS total_orfaos,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_uf u
LEFT JOIN dim_regiao r ON u.id_regiao = r.id_regiao
WHERE r.id_regiao IS NULL;

-- [T06] Municípios sem UF (ESPERADO: 0)
SELECT 'Municípios sem UF' AS teste, COUNT(*) AS total_orfaos,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM dim_municipio m
LEFT JOIN dim_uf u ON m.id_uf = u.id_uf
WHERE u.id_uf IS NULL;

-- [T07] Fatos com município inexistente (ESPERADO: 0)
SELECT 'Fatos sem município' AS teste, COUNT(*) AS total_orfaos,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM fato_indicador_municipio f
LEFT JOIN dim_municipio m ON f.id_municipio = m.id_municipio
WHERE m.id_municipio IS NULL;

-- [T08] Fatos com variável inexistente (ESPERADO: 0)
SELECT 'Fatos sem variável' AS teste, COUNT(*) AS total_orfaos,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM fato_indicador_municipio f
LEFT JOIN dim_variavel v ON f.id_variavel = v.id_variavel
WHERE v.id_variavel IS NULL;


-- ================================================================
-- SEÇÃO 3 — QUALIDADE DA FATO
-- ================================================================

-- [T09] Total de registros (ESPERADO: >= 100.000)
SELECT COUNT(*) AS total_fatos,
    CASE WHEN COUNT(*) >= 100000 THEN '✅ OK' ELSE '⚠️ VOLUME BAIXO' END AS status
FROM fato_indicador_municipio;

-- [T10] Anos disponíveis (ESPERADO: 2020, 2021, 2022)
SELECT DISTINCT ano FROM fato_indicador_municipio ORDER BY ano;

-- [T11] Registros por variável
SELECT
    v.nome_variavel, v.unidade,
    COUNT(*)                       AS total_registros,
    COUNT(DISTINCT f.id_municipio) AS municipios_cobertos,
    COUNT(DISTINCT f.ano)          AS anos_cobertos
FROM fato_indicador_municipio f
JOIN dim_variavel v ON f.id_variavel = v.id_variavel
GROUP BY v.nome_variavel, v.unidade
ORDER BY total_registros DESC;

-- [T12] Valores nulos (ESPERADO: 0)
SELECT 'Valores nulos' AS teste, COUNT(*) AS total,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
FROM fato_indicador_municipio WHERE valor IS NULL;

-- [T13] Valores negativos (ESPERADO: 0)
SELECT 'Valores negativos' AS teste, COUNT(*) AS total,
    CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '⚠️ VERIFICAR' END AS status
FROM fato_indicador_municipio WHERE valor < 0;


-- ================================================================
-- SEÇÃO 4 — SANIDADE DOS VALORES
-- ================================================================

-- [T14] Estatísticas do PIB por ano
-- ESPERADO: valores crescentes; máximo ~800.000.000 (SP)
SELECT
    f.ano,
    TO_CHAR(ROUND(AVG(f.valor)::numeric, 0), 'FM999,999,999') AS media,
    TO_CHAR(MIN(f.valor)::numeric, 'FM999,999,999')           AS minimo,
    TO_CHAR(MAX(f.valor)::numeric, 'FM999,999,999')           AS maximo,
    COUNT(DISTINCT f.id_municipio)                            AS municipios
FROM fato_indicador_municipio f
JOIN dim_variavel v ON f.id_variavel = v.id_variavel
WHERE v.nome_variavel = 'PIB'
GROUP BY f.ano ORDER BY f.ano;

-- [T15] Top 10 municípios por PIB — ano mais recente
-- ESPERADO: São Paulo no topo
SELECT
    ROW_NUMBER() OVER (ORDER BY f.valor DESC) AS pos,
    m.nome_municipio,
    u.sigla_uf,
    TO_CHAR(f.valor::numeric, 'FM999,999,999') AS pib
FROM fato_indicador_municipio f
JOIN dim_municipio m ON f.id_municipio = m.id_municipio
JOIN dim_uf u        ON m.id_uf = u.id_uf
JOIN dim_variavel v  ON f.id_variavel = v.id_variavel
WHERE v.nome_variavel = 'PIB'
  AND f.ano = (SELECT MAX(ano) FROM fato_indicador_municipio)
ORDER BY f.valor DESC LIMIT 10;

-- [T16] PIB por região — ano mais recente
-- ESPERADO: Sudeste ~55%, Sul ~17%, NE ~14%, CO ~10%, N ~5%
SELECT
    r.nome_regiao,
    TO_CHAR(SUM(f.valor)::numeric, 'FM999,999,999,999') AS pib_total,
    ROUND(100.0 * SUM(f.valor) / SUM(SUM(f.valor)) OVER (), 1) AS pct
FROM fato_indicador_municipio f
JOIN dim_municipio m ON f.id_municipio = m.id_municipio
JOIN dim_uf u        ON m.id_uf = u.id_uf
JOIN dim_regiao r    ON u.id_regiao = r.id_regiao
JOIN dim_variavel v  ON f.id_variavel = v.id_variavel
WHERE v.nome_variavel = 'PIB'
  AND f.ano = (SELECT MAX(ano) FROM fato_indicador_municipio)
GROUP BY r.nome_regiao
ORDER BY SUM(f.valor) DESC;


-- ================================================================
-- SEÇÃO 5 — VALIDAÇÃO DAS VIEWS MART
-- ================================================================

-- [T17] Contagem de todas as views (ESPERADO: todas > 0)
SELECT 'mart.pib_por_municipio'      AS view_mart, COUNT(*) AS total,
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END AS status
FROM mart.pib_por_municipio
UNION ALL
SELECT 'mart.pib_por_uf_ano',         COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.pib_por_uf_ano
UNION ALL
SELECT 'mart.pib_por_regiao_ano',     COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.pib_por_regiao_ano
UNION ALL
SELECT 'mart.concentracao_municipio', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.concentracao_municipio
UNION ALL
SELECT 'mart.composicao_vab_uf_ano',  COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.composicao_vab_uf_ano
UNION ALL
SELECT 'mart.ranking_municipios',     COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.ranking_municipios
UNION ALL
SELECT 'mart.concentracao_uf_metrics', COUNT(*),
    CASE WHEN COUNT(*) > 0 THEN '✅ OK' ELSE '❌ VAZIA' END
FROM mart.concentracao_uf_metrics;

-- [T18] Preview ranking — top 5 com posição calculada
-- ESPERADO: São Paulo no topo (pos = 1)
SELECT
    r.ranking_nacional AS pos,
    r.nome_municipio,
    r.sigla_uf,
    TO_CHAR(r.pib::numeric, 'FM999,999,999') AS pib
FROM mart.ranking_municipios r
WHERE r.ano = (SELECT MAX(ano) FROM fato_indicador_municipio)
ORDER BY r.ranking_nacional
LIMIT 5;

-- [T19] Consistência mart vs fato — PIB de SP deve ser IGUAL
SELECT 'mart'        AS fonte,
    TO_CHAR(pib_total::numeric, 'FM999,999,999,999') AS pib_sp
FROM mart.pib_por_uf_ano
WHERE sigla_uf = 'SP'
  AND ano = (SELECT MAX(ano) FROM fato_indicador_municipio)
UNION ALL
SELECT 'fato direto',
    TO_CHAR(SUM(f.valor)::numeric, 'FM999,999,999,999')
FROM fato_indicador_municipio f
JOIN dim_municipio m ON f.id_municipio = m.id_municipio
JOIN dim_uf u        ON m.id_uf = u.id_uf
JOIN dim_variavel v  ON f.id_variavel = v.id_variavel
WHERE u.sigla_uf = 'SP'
  AND v.nome_variavel LIKE 'SIDRA:37 -%'
  AND f.ano = (SELECT MAX(ano) FROM fato_indicador_municipio);


-- ================================================================
-- SEÇÃO 6 — VALIDAÇÃO DE ÍNDICES
-- ================================================================

-- [T20] Listar todos os índices criados (ESPERADO: 12+ índices)
SELECT 
    indexname,
    tablename,
    CASE 
        WHEN indexname LIKE '%uid' THEN '🔑 UNIQUE'
        ELSE '📇 Regular'
    END AS tipo
FROM pg_indexes
WHERE schemaname IN ('public', 'mart')
  AND tablename NOT LIKE 'pg_%'
ORDER BY tablename, indexname;

-- [T21] Validar índices UNIQUE em views (ESPERADO: 7)
SELECT COUNT(*) AS qtd_unique_indexes,
    CASE 
        WHEN COUNT(*) >= 7 THEN '✅ OK'
        ELSE '❌ FALHOU'
    END AS status
FROM pg_indexes
WHERE schemaname = 'mart'
  AND indexname LIKE '%uid';


-- ================================================================
-- SEÇÃO 7 — VALIDAÇÃO DA FUNÇÃO REFRESH
-- ================================================================

-- [T22] Testar que a função refresh_all existe e é executável
SELECT 'mart.refresh_all()' AS funcao,
    CASE 
        WHEN EXISTS (
            SELECT 1 FROM information_schema.routines 
            WHERE routine_name = 'refresh_all' AND routine_schema = 'mart'
        ) THEN '✅ EXISTE'
        ELSE '❌ NÃO EXISTE'
    END AS status;

-- [T23] Executar refresh (validar que executa sem erro)
-- Descomente se quiser testar (leva 2-3 minutos)
-- SELECT mart.refresh_all();


-- ================================================================
-- SEÇÃO 8 — TESTES DE PERFORMANCE
-- ================================================================

-- ⏱️ Use \timing on para ver duração em ms

-- [T24] Query rápida: pib_por_municipio
-- Esperado: < 50ms
-- SELECT COUNT(*) FROM mart.pib_por_municipio WHERE ano = 2021;

-- [T25] Query rápida: pib_por_uf_ano  
-- Esperado: < 20ms
-- SELECT * FROM mart.pib_por_uf_ano WHERE ano = 2021 LIMIT 1;

-- [T26] Query rápida: concentracao_uf_metrics
-- Esperado: < 15ms
-- SELECT * FROM mart.concentracao_uf_metrics LIMIT 1;


-- ================================================================
-- SEÇÃO 9 — DUPLICATAS
-- ================================================================

-- [T20] Duplicatas na PK da fato (ESPERADO: 0 linhas)
SELECT id_municipio, id_variavel, ano, COUNT(*) AS ocorrencias
FROM fato_indicador_municipio
GROUP BY id_municipio, id_variavel, ano
HAVING COUNT(*) > 1
ORDER BY ocorrencias DESC;

-- [T21] Municípios duplicados na mesma UF (ESPERADO: 0 linhas)
SELECT u.sigla_uf, m.nome_municipio, COUNT(*) AS ocorrencias
FROM dim_municipio m
JOIN dim_uf u ON m.id_uf = u.id_uf
GROUP BY u.sigla_uf, m.nome_municipio
HAVING COUNT(*) > 1;


-- ================================================================
-- SEÇÃO 10 — RESUMO FINAL (rode por último)
-- ================================================================
-- ESPERADO: todas as linhas com ✅ OK

SELECT teste, total, status FROM (
    SELECT 1, 'T01 dim_regiao (= 5)'                    AS teste, COUNT(*) AS total,
        CASE WHEN COUNT(*) = 5 THEN '✅ OK' ELSE '❌ FALHOU' END AS status
    FROM dim_regiao
    UNION ALL
    SELECT 2, 'T02 dim_uf (= 27)',                       COUNT(*),
        CASE WHEN COUNT(*) = 27 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM dim_uf
    UNION ALL
    SELECT 3, 'T03 dim_municipio (5560–5572)',           COUNT(*),
        CASE WHEN COUNT(*) BETWEEN 5560 AND 5572 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM dim_municipio
    UNION ALL
    SELECT 4, 'T04 dim_variavel (>= 5)',                 COUNT(*),
        CASE WHEN COUNT(*) >= 5 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM dim_variavel
    UNION ALL
    SELECT 5, 'T09 fato total (>= 100k)',                COUNT(*),
        CASE WHEN COUNT(*) >= 100000 THEN '✅ OK' ELSE '⚠️ VOLUME BAIXO' END
    FROM fato_indicador_municipio
    UNION ALL
    SELECT 6, 'T12 fato nulos (= 0)',                    COUNT(*),
        CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM fato_indicador_municipio WHERE valor IS NULL
    UNION ALL
    SELECT 7, 'T13 fato negativos (= 0)',                COUNT(*),
        CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '⚠️ VERIFICAR' END
    FROM fato_indicador_municipio WHERE valor < 0
    UNION ALL
    SELECT 8, 'T20 duplicatas fato (= 0)',               COUNT(*),
        CASE WHEN COUNT(*) = 0 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM (
        SELECT id_municipio FROM fato_indicador_municipio
        GROUP BY id_municipio, id_variavel, ano HAVING COUNT(*) > 1
    ) d
    UNION ALL
    SELECT 9, 'T17 views materializadas (7)',            COUNT(*),
        CASE WHEN COUNT(*) = 7 THEN '✅ OK' ELSE '❌ FALHOU' END
    FROM (
        SELECT 1 FROM mart.pib_por_municipio
        UNION ALL SELECT 1 FROM mart.pib_por_uf_ano
        UNION ALL SELECT 1 FROM mart.pib_por_regiao_ano
        UNION ALL SELECT 1 FROM mart.concentracao_municipio
        UNION ALL SELECT 1 FROM mart.composicao_vab_uf_ano
        UNION ALL SELECT 1 FROM mart.ranking_municipios
        UNION ALL SELECT 1 FROM mart.concentracao_uf_metrics
    ) views
    UNION ALL
    SELECT 10, 'T21 índices UNIQUE (7)',                 COUNT(*),
        CASE WHEN COUNT(*) >= 7 THEN '✅ OK' ELSE '⚠️ VERIFICAR' END
    FROM pg_indexes
    WHERE schemaname = 'mart' AND indexname LIKE '%uid'
    UNION ALL
    SELECT 11, 'T22 função refresh_all',                 1,
        CASE 
            WHEN EXISTS (SELECT 1 FROM information_schema.routines 
                        WHERE routine_name = 'refresh_all' AND routine_schema = 'mart')
            THEN '✅ OK' ELSE '❌ NÃO EXISTE' 
        END
) resumo
ORDER BY 1;

-- ================================================================
-- FIM DO SCRIPT
-- ================================================================

SELECT
    v.nome_variavel,
    COUNT(*)       AS qtd_negativos,
    MIN(f.valor)   AS menor_valor,
    MAX(f.valor)   AS maior_valor
FROM fato_indicador_municipio f
JOIN dim_variavel v ON f.id_variavel = v.id_variavel
WHERE f.valor < 0
GROUP BY v.nome_variavel
ORDER BY qtd_negativos DESC;
