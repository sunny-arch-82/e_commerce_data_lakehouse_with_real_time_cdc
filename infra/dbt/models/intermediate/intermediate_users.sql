{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- User dimension: latest profile state, enriched with purchase summary
SELECT
    u.id                            AS user_id,
    u.first_name,
    u.last_name,
    u.first_name || ' ' || u.last_name AS full_name,
    u.email,
    u.age,
    u.gender,
    u.street_address,
    u.postal_code,
    u.city,
    u.state,
    u.country,
    u.latitude,
    u.longitude,
    u.traffic_source,
    u.created_at                    AS registered_at,
    u.updated_at,
    u.kafka_ts

FROM {{ ref('staging_users') }} u

{% if is_incremental() %}
WHERE u.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
