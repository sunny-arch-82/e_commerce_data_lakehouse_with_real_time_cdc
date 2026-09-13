{{ config(
    materialized='incremental',
    unique_key='date_key',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

WITH date_spine AS (
    SELECT date_add('day', n, DATE '2020-01-01') AS full_date
    FROM UNNEST(sequence(0, 3650)) AS t(n)
)

SELECT
    CAST(date_format(full_date, '%Y%m%d') AS INTEGER)                   AS date_key,
    full_date,
    day_of_week(full_date)                                              AS day_of_week,
    date_format(full_date, '%W')                                        AS day_name,
    week_of_year(full_date)                                             AS week_of_year,
    month(full_date)                                                    AS month_num,
    date_format(full_date, '%M')                                        AS month_name,
    quarter(full_date)                                                  AS quarter,
    year(full_date)                                                     AS year,
    CASE WHEN day_of_week(full_date) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine

{% if is_incremental() %}
WHERE full_date > (SELECT MAX(full_date) FROM {{ this }})
{% endif %}
