{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Product dimension: latest catalog state with distribution center info
SELECT
    p.id                            AS product_id,
    p.name                          AS product_name,
    p.category                      AS product_category,
    p.department                    AS product_department,
    p.brand                         AS product_brand,
    p.sku,
    p.cost,
    p.retail_price,
    p.retail_price - p.cost         AS list_margin,
    p.distribution_center_id,
    dc.name                         AS distribution_center,
    dc.latitude                     AS dc_latitude,
    dc.longitude                    AS dc_longitude,
    p.event_ts_ms

FROM {{ ref('staging_products') }} p
LEFT JOIN {{ ref('staging_dist_centers') }} dc ON p.distribution_center_id = dc.id

{% if is_incremental() %}
WHERE p.event_ts_ms > (SELECT MAX(event_ts_ms) FROM {{ this }})
{% endif %}
