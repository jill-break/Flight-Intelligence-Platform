-- ═══════════════════════════════════════════════════════
-- Flight Intelligence Platform — Analytics DB Init
-- ═══════════════════════════════════════════════════════
-- This script runs automatically when postgres_analytics
-- starts for the first time (via docker-entrypoint-initdb.d).
-- It creates the schema that Spark writes into.

CREATE SCHEMA IF NOT EXISTS flight_analytics;

-- Grant usage to the default user
GRANT ALL ON SCHEMA flight_analytics TO PUBLIC;
