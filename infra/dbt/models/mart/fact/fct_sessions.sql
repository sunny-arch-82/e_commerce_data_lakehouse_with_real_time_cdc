{{ config(
    materialized='incremental',
    unique_key='session_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per session
-- Safety window: reprocess sessions with events in last 2h to handle sessions spanning batch boundaries

{% if is_incremental() %}
WITH watermark AS (
    SELECT COALESCE(MAX(_dwh_updated_at), TIMESTAMP '1970-01-01') - INTERVAL '2' HOUR AS cutoff FROM {{ this }}
),
recent_session_ids AS (
    SELECT DISTINCT e.session_id
    FROM {{ ref('intermediate_events') }} e
    CROSS JOIN watermark w
    WHERE e.kafka_ts > w.cutoff
)
{% endif %}

SELECT
    session_id,
    MAX(user_id)                                                        AS user_id,
    CAST(date_format(date_trunc('day', MIN(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000)))), '%Y%m%d') AS INTEGER) AS date_key,
    MAX(traffic_source)                                                 AS traffic_source,
    MAX(browser)                                                        AS browser,
    MAX(city)                                                           AS city,
    MAX(state)                                                          AS state,
    MAX(customer_country)                                               AS customer_country,
    bool_or(is_ghost)                                                   AS is_ghost,
    -- Funnel stages
    MAX(CASE WHEN event_type = 'home'                      THEN 1 ELSE 0 END) AS hit_home,
    MAX(CASE WHEN event_type IN ('department', 'category') THEN 1 ELSE 0 END) AS hit_browse,
    MAX(CASE WHEN event_type = 'product'                   THEN 1 ELSE 0 END) AS hit_product,
    MAX(CASE WHEN event_type = 'cart'                      THEN 1 ELSE 0 END) AS hit_cart,
    MAX(CASE WHEN event_type = 'purchase'                  THEN 1 ELSE 0 END) AS hit_purchase,
    MAX(CASE WHEN event_type = 'cancel'                    THEN 1 ELSE 0 END) AS hit_cancel,
    MAX(CASE WHEN event_type = 'return'                    THEN 1 ELSE 0 END) AS hit_return,
    -- Date
    TRY(CAST(MIN(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000)) AS DATE)) AS session_date,
    -- Measures
    COUNT(event_id)                                                     AS total_events,
    MIN(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000))) AS session_start_at,
    MAX(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000))) AS session_end_at,
    date_diff('second', MIN(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000))), MAX(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000)))) AS session_duration_seconds,
    -- Watermark for incremental runs
    MAX(TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000))) AS last_event_ts,
    MAX(e.kafka_ts)                                                             AS _dwh_updated_at

FROM {{ ref('intermediate_events') }} e

{% if is_incremental() %}
WHERE session_id IN (SELECT session_id FROM recent_session_ids)
{% endif %}

GROUP BY 1
