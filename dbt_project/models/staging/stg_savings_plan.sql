{{ config(materialized='view') }}
SELECT
    plan_id,
    customer_uid AS user_id,
    product_type AS plan_type,
    amount AS target_amount,
    start_date,
    end_date,
    frequency,
    status,
    created_at,
    updated_at,
    extracted_at
FROM analytics.raw_savings_plan
WHERE deleted_at IS NULL