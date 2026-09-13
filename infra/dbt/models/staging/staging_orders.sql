{{ config(materialized='ephemeral') }}

-- Latest state of each order (deduplicate CDC via kafka_ts watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) AS rn
    FROM {{ source('staging', 'orders') }}
)
WHERE rn = 1
