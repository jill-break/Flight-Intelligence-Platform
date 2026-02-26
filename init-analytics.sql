-- ═══════════════════════════════════════════════════════
-- Flight Intelligence Platform — Analytics DB Init
-- ═══════════════════════════════════════════════════════
-- This script runs automatically when postgres_analytics
-- starts for the first time (via docker-entrypoint-initdb.d).
-- It creates the schema that Spark writes into.

CREATE SCHEMA IF NOT EXISTS flight_analytics;

-- Grant usage to the default user
GRANT ALL ON SCHEMA flight_analytics TO PUBLIC;

-- ═══════════════════════════════════════════════════════
-- Quarantine Log — tracks dropped/cleaned rows
-- ═══════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS flight_analytics.quarantine_log (
    id SERIAL PRIMARY KEY,
    source_filename VARCHAR(255) NOT NULL,
    transaction_id VARCHAR(255),
    drop_reason TEXT NOT NULL,
    cleaned_at TIMESTAMP DEFAULT NOW()
);

GRANT ALL ON TABLE flight_analytics.quarantine_log TO PUBLIC;
