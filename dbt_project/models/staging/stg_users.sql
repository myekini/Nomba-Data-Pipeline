{{ config(materialized='view') }}

WITH source AS (
    SELECT
        uid,
        first_name,
        last_name,
        occupation,
        state,
        extracted_at,
        updated_at
    FROM analytics.raw_users
)

SELECT
    uid AS user_id,
    INITCAP(TRIM(first_name)) AS first_name,
    INITCAP(TRIM(last_name)) AS last_name,
    LOWER(TRIM(occupation)) AS occupation,
    UPPER(TRIM(state)) AS state_code,
    COALESCE(updated_at, extracted_at) AS record_timestamp
FROM source
WHERE uid IS NOT NULL

