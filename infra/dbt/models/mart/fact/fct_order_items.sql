{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: 1 row per order item — most granular fact, FK to all dims
SELECT
    -- Keys
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    CAST(date_format(date_trunc('day', TRY(from_unixtime(CAST(NULLIF(oi.item_created_at, '') AS DOUBLE) / 1000000))), '%Y%m%d') AS INTEGER) AS date_key,
    -- Status
    oi.order_status,
    oi.item_status,
    -- Measures
    oi.quantity,
    oi.sale_price,
    oi.revenue,
    oi.gross_margin,
    oi.product_cost,
    -- Dates and timestamps (Avro stores as ISO string)
    TRY(CAST(from_unixtime(CAST(NULLIF(oi.order_created_at, '') AS DOUBLE) / 1000000) AS DATE))                         AS order_date,
    TRY(CAST(from_unixtime(CAST(NULLIF(oi.item_created_at, '') AS DOUBLE) / 1000000) AS DATE))                          AS item_date,
    TRY(from_unixtime(CAST(NULLIF(oi.order_created_at, '') AS DOUBLE) / 1000000))                    AS order_created_at,
    TRY(from_unixtime(CAST(NULLIF(oi.item_created_at, '') AS DOUBLE) / 1000000))                      AS item_created_at,
    TRY(from_unixtime(CAST(NULLIF(oi.item_shipped_at, '') AS DOUBLE) / 1000000))                    AS item_shipped_at,
    TRY(from_unixtime(CAST(NULLIF(oi.item_delivered_at, '') AS DOUBLE) / 1000000))                   AS item_delivered_at,
    TRY(from_unixtime(CAST(NULLIF(oi.item_returned_at, '') AS DOUBLE) / 1000000))                    AS item_returned_at,
    TRY(from_unixtime(CAST(NULLIF(oi.item_cancelled_at, '') AS DOUBLE) / 1000000))                   AS item_cancelled_at,
    oi.kafka_ts                                                                                      AS _dwh_updated_at

FROM {{ ref('intermediate_order_items') }} oi

{% if is_incremental() %}
WHERE oi.kafka_ts > (SELECT COALESCE(MAX(_dwh_updated_at), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
