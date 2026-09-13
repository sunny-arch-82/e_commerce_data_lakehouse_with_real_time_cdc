{{ config(materialized='ephemeral') }}

-- Latest state of each distribution center (deduplicate via event_ts_ms watermark)
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY event_ts_ms DESC) AS rn
    FROM {{ source('staging', 'dist_centers') }}
)
WHERE rn = 1
