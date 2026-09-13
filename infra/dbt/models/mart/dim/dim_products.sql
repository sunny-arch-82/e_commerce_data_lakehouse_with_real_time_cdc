{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    p.product_category,
    p.product_department,
    p.product_brand,
    p.sku,
    p.cost,
    p.retail_price,
    p.list_margin,
    CASE
        WHEN p.retail_price >= 100 THEN 'premium'
        WHEN p.retail_price >= 50  THEN 'mid'
        ELSE 'budget'
    END                         AS price_tier,
    p.distribution_center_id,
    p.distribution_center,
    p.dc_latitude,
    p.dc_longitude

FROM {{ ref('intermediate_products') }} p
