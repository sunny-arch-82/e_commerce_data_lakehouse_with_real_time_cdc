{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

SELECT
    o.id                                AS order_id,
    o.status                            AS order_status,
    o.num_of_items,
    o.created_at,
    o.updated_at,
    o.shipped_at,
    o.delivered_at,
    o.returned_at,
    o.cancelled_at,
    -- User
    o.user_id,
    u.first_name || ' ' || u.last_name  AS customer_name,
    u.gender                            AS customer_gender,
    u.age                               AS customer_age,
    u.country                           AS customer_country,
    u.state                             AS customer_state,
    u.city                              AS customer_city,
    u.latitude                          AS customer_lat,
    u.longitude                         AS customer_lon,
    u.created_at                        AS user_registered_at,
    u.traffic_source,
    -- Metadata
    o.kafka_ts

FROM {{ ref('staging_orders') }} o
INNER JOIN {{ ref('staging_users') }} u ON o.user_id = u.id
{% if is_incremental() %}
WHERE o.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
