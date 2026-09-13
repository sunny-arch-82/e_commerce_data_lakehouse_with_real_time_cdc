{{ config(materialized='ephemeral') }}

-- Web/app events — append-only, no dedup needed
SELECT * FROM {{ source('staging', 'events') }}
