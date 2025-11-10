{{ config(
    materialized='incremental',
    unique_key='txn_id'
) }}

WITH tx AS (
    SELECT
        txn_id,
        plan_id,
        user_id,
        amount,
        currency,
        side,
        rate,
        txn_timestamp,
        is_deleted
    FROM {{ ref('stg_savings_transactions') }}
    WHERE NOT is_deleted
),

users AS (
    SELECT
        user_key,
        user_id
    FROM {{ ref('dim_users') }}
    WHERE is_current = TRUE
),

joined AS (
    SELECT
        t.txn_id,
        u.user_key,
        t.plan_id,
        t.amount,
        t.currency,
        t.side,
        t.rate,
        t.txn_timestamp,
        DATE(t.txn_timestamp) AS transaction_date
    FROM tx t
    LEFT JOIN users u ON t.user_id = u.user_id
)

SELECT * FROM joined

{% if is_incremental() %}
WHERE txn_timestamp > (SELECT COALESCE(MAX(txn_timestamp), '1970-01-01') FROM {{ this }})
{% endif %}