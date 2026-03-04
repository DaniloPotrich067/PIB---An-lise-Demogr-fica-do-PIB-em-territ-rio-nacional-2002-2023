-- RESET TOTAL (PANICO)
-- Use apenas para limpar o banco e recomeçar do zero.
DROP SCHEMA IF EXISTS public CASCADE;
DROP SCHEMA IF EXISTS mart CASCADE;
CREATE SCHEMA IF NOT EXISTS public;
CREATE SCHEMA IF NOT EXISTS mart; -- Schema para views materializadas analíticas
