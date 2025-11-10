{{ config(materialized='incremental', unique_key='txn_id') }}
WITH base AS (
    SELECT
        t.txn_id,
        t.plan_id,
        p.customer_uid AS user_id,
        t.amount,
        LOWER(t.side) AS side,
        UPPER(t.currency) AS currency,
        t.rate,
        t.txn_timestamp,
        t.updated_at,
        t.deleted_at
    FROM analytics.raw_savingstransaction t
    LEFT JOIN analytics.raw_savings_plan p 
        ON t.plan_id::text = p.plan_id::text  -- ✅ Cast both to text
)
SELECT
    txn_id,
    plan_id,
    user_id,
    amount,
    side,
    currency,
    rate,
    txn_timestamp,
    updated_at,
    deleted_at,
    (deleted_at IS NOT NULL) AS is_deleted
FROM base
{% if is_incremental() %}
WHERE
    updated_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
    OR deleted_at > (SELECT COALESCE(MAX(updated_at), '1970-01-01') FROM {{ this }})
{% endif %}