
-- NOMBA ANALYTICS WAREHOUSE SCHEMA

-- Create the analytics schema if it does not exist
CREATE SCHEMA IF NOT EXISTS analytics;

-- ----------------------------------------------------------
-- RAW LAYER TABLES (landing zone for CDC from MongoDB + Aiven)
-- ----------------------------------------------------------

-- Users from MongoDB Atlas
CREATE TABLE IF NOT EXISTS analytics.raw_users (
    uid TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    occupation TEXT,
    state TEXT,
    record_hash TEXT,
    extracted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- Savings plans from Aiven PostgreSQL
CREATE TABLE IF NOT EXISTS analytics.raw_savings_plan (
    plan_id UUID PRIMARY KEY,
    product_type TEXT,
    customer_uid TEXT,
    amount NUMERIC(15, 2),
    frequency TEXT,
    start_date DATE,
    end_date DATE,
    status TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    extracted_at TIMESTAMPTZ
);

-- Savings transactions from Aiven PostgreSQL
CREATE TABLE IF NOT EXISTS analytics.raw_savingstransaction (
    txn_id UUID PRIMARY KEY,
    plan_id UUID,
    amount NUMERIC(15, 2),
    currency TEXT,
    side TEXT,
    rate NUMERIC(10, 2),
    txn_timestamp TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ,
    extracted_at TIMESTAMPTZ
);

-- ----------------------------------------------------------
-- CDC METADATA TABLE (tracks incremental extraction state)
-- ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.cdc_metadata (
    source_name TEXT PRIMARY KEY,
    last_extracted_timestamp TIMESTAMPTZ,
    last_extraction_status TEXT,
    records_extracted BIGINT,
    updated_at TIMESTAMPTZ
);

-- ----------------------------------------------------------
-- SEED INITIAL METADATA ROWS
-- ----------------------------------------------------------

INSERT INTO analytics.cdc_metadata (
    source_name,
    last_extracted_timestamp,
    last_extraction_status,
    records_extracted,
    updated_at
)
VALUES
    ('mongodb_users', '1970-01-01', 'never_run', 0, NOW()),
    ('postgres_savings_plan', '1970-01-01', 'never_run', 0, NOW()),
    ('postgres_savingstransaction', '1970-01-01', 'never_run', 0, NOW())
ON CONFLICT (source_name) DO NOTHING;
