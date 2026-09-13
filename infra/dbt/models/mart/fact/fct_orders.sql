{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order
SELECT
    -- Keys
    o.order_id,
    o.user_id,
    CAST(date_format(date_trunc('day', TRY(from_unixtime(CAST(NULLIF(o.created_at, '') AS DOUBLE) / 1000000))), '%Y%m%d') AS INTEGER) AS date_key,
    -- Attributes
    o.order_status,
    o.num_of_items,
    o.traffic_source,
    -- Dates and timestamps (Avro stores as ISO string)
    TRY(CAST(from_unixtime(CAST(NULLIF(o.created_at, '') AS DOUBLE) / 1000000) AS DATE)) AS order_date,
    TRY(from_unixtime(CAST(NULLIF(o.created_at, '') AS DOUBLE) / 1000000))               AS order_created_at,
    TRY(from_unixtime(CAST(NULLIF(o.shipped_at, '') AS DOUBLE) / 1000000))               AS shipped_at,
    TRY(from_unixtime(CAST(NULLIF(o.delivered_at, '') AS DOUBLE) / 1000000))             AS delivered_at,
    TRY(from_unixtime(CAST(NULLIF(o.returned_at, '') AS DOUBLE) / 1000000))              AS returned_at,
    TRY(from_unixtime(CAST(NULLIF(o.cancelled_at, '') AS DOUBLE) / 1000000))             AS cancelled_at,
    o.kafka_ts                                                                           AS _dwh_updated_at

FROM {{ ref('intermediate_orders') }} o

{% if is_incremental() %}
WHERE o.kafka_ts > (SELECT COALESCE(MAX(_dwh_updated_at), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
