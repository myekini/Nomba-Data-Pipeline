{{ config(materialized='table') }}

WITH dates AS (
    SELECT
        dd::date AS date_day
    FROM generate_series('2020-01-01', '2030-12-31', interval '1 day') AS dd
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day)::int AS year,
    EXTRACT(MONTH FROM date_day)::int AS month,
    EXTRACT(DAY FROM date_day)::int AS day,
    TO_CHAR(date_day, 'Day') AS day_name,
    TO_CHAR(date_day, 'Mon') AS month_name
FROM dates
