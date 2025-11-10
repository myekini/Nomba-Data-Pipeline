{{ config(materialized='view') }}

-- Check data freshness across all raw tables
WITH user_freshness AS (
    SELECT
        'raw_users' AS table_name,
        MAX(extracted_at) AS last_update,
        CURRENT_TIMESTAMP - MAX(extracted_at) AS staleness
    FROM analytics.raw_users
),
plan_freshness AS (
    SELECT
        'raw_savings_plan' AS table_name,
        MAX(extracted_at) AS last_update,
        CURRENT_TIMESTAMP - MAX(extracted_at) AS staleness
    FROM analytics.raw_savings_plan
),
txn_freshness AS (
    SELECT
        'raw_savingstransaction' AS table_name,
        MAX(extracted_at) AS last_update,
        CURRENT_TIMESTAMP - MAX(extracted_at) AS staleness
    FROM analytics.raw_savingstransaction
)

-- Combine all freshness checks
SELECT * FROM user_freshness
UNION ALL
SELECT * FROM plan_freshness
UNION ALL
SELECT * FROM txn_freshness