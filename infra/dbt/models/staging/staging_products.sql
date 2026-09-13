{{ config(materialized='ephemeral') }}

-- Latest state of each product (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM {{ source('staging', 'products') }}
)
WHERE rn = 1
