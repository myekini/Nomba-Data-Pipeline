{{ config(materialized='table') }}

WITH current_snapshot AS (
    SELECT
        user_id,
        first_name,
        last_name,
        occupation,
        state_code,
        record_timestamp
    FROM {{ ref('stg_users') }}
),

ranked AS (
    SELECT
        user_id,
        first_name,
        last_name,
        occupation,
        state_code,
        record_timestamp,
        LAG(record_timestamp) OVER (PARTITION BY user_id ORDER BY record_timestamp) AS prev_ts
    FROM current_snapshot
)

SELECT
    -- in practice use a surrogate key macro; keeping it readable here
    user_id || '_' || TO_CHAR(record_timestamp, 'YYYYMMDDHH24MISS') AS user_key,
    user_id,
    first_name,
    last_name,
    occupation,
    state_code,
    COALESCE(prev_ts, record_timestamp) AS effective_start_date,
    '9999-12-31'::date AS effective_end_date,
    TRUE AS is_current
FROM ranked
