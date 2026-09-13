{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per event
SELECT
    -- Keys
    e.event_id,
    e.user_id,
    e.session_id,
    CAST(date_format(date_trunc('day', TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000))), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    e.sequence_number,
    e.event_type,
    e.uri,
    e.traffic_source,
    e.browser,
    e.ip_address,
    -- Location
    e.city,
    e.state,
    e.postal_code,
    -- Flags
    e.is_ghost,
    -- Timestamps
    TRY(CAST(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000) AS DATE)) AS event_date,
    TRY(from_unixtime(CAST(NULLIF(e.event_time, '') AS DOUBLE) / 1000000)) AS event_time_ts,
    e.kafka_ts                                                          AS _dwh_updated_at

FROM {{ ref('intermediate_events') }} e

{% if is_incremental() %}
WHERE e.kafka_ts > (SELECT COALESCE(MAX(_dwh_updated_at), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
