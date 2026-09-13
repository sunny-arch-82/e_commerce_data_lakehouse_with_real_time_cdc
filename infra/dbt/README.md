# dbt — Transformation Models

**Project:** `infra/dbt/`

## Materialization Strategy

| Layer | Materialization | When data changes | Notes |
|-------|----------------|-------------------|-------|
| `staging/` | `ephemeral` | Never materializes | Rendered as CTEs; dedup in downstream |
| `intermediate/` | `incremental` | Only new CDC records | `delete+insert` on `unique_key` |
| `mart/dim/` | `table` | Full rewrite | SCD Type 1 |
| `mart/fact/` | `table` | Full rewrite | Atomic transactions |

## Default Config (`infra/dbt/dbt_project.yml`)

```yaml
models:
  ecommerce_cdc_lakehouse:
    staging:
      +schema: staging
      +materialized: ephemeral
    intermediate:
      +schema: intermediate
      +materialized: incremental
      +incremental_strategy: delete+insert
      +on_schema_change: append_new_columns
    mart:
      +schema: mart
      +materialized: table
      +on_table_exists: drop
      dim:
        +materialized: table
        +on_table_exists: drop
      fact:
        +materialized: table
        +on_table_exists: drop

on-run-start:
  - "CREATE SCHEMA IF NOT EXISTS staging WITH (location = 's3a://lakehouse')"
  - "CREATE SCHEMA IF NOT EXISTS intermediate WITH (location = 's3a://lakehouse')"
  - "CREATE SCHEMA IF NOT EXISTS mart WITH (location = 's3a://lakehouse')"
```

## on-run-start Hook

Ensures schemas exist with correct MinIO paths **before** any model runs.

> **Why?** Without this, Trino auto-creates schemas at `s3a://lakehouse/warehouse/` (default path). HMS stores that path, but notebook writes to `lakehouse/`. Result: `DELTA_LAKE_INVALID_SCHEMA: Metadata not found`.

## Staging Models (ephemeral)

All staging models are `ephemeral` — they exist only as SQL CTEs in dependent models:

```sql
-- infra/dbt/models/staging/staging_events.sql
{{ config(materialized='ephemeral') }}

SELECT * FROM {{ source('staging', 'events') }}
```

Deduplication happens in `intermediate_*` models via `ROW_NUMBER()` window.

## Intermediate Models (incremental)

```sql
-- infra/dbt/models/intermediate/intermediate_order_items.sql
{{ config(
    materialized='incremental',
    unique_key='order_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

SELECT
    oi.id                            AS order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.sale_price,
    oi.kafka_ts
FROM {{ ref('staging_order_items') }} oi
{% if is_incremental() %}
WHERE oi.kafka_ts > (SELECT MAX(kafka_ts) FROM {{ this }})
{% endif %}
```

**How incremental works:**
1. `is_incremental()` returns `True` if table exists
2. `WHERE kafka_ts > MAX(kafka_ts)` filters to only new CDC records
3. `delete+insert` strategy deletes rows matching `unique_key`, then inserts all — latest state preserved

## Schema Routing

The `+schema` config routes each model to its target schema. The macro `macros/generate_schema_name.sql` implements this:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

> Do NOT hardcode schema names in model SQL. Use `{{ ref('model_name') }}` and `{{ source('staging', 'table_name') }}` instead.

## Mart Models

### dim_customers — SCD Type 1 with Dual Watermark

Only recalculates customers affected by recent CDC events:

```sql
-- infra/dbt/models/mart/dim/dim_customers.sql
{{ config(materialized='table', on_table_exists='drop') }}

WITH changed_users AS (
    SELECT * FROM {{ ref('intermediate_users') }}
    WHERE kafka_ts > GREATEST(
        (SELECT MAX(kafka_ts) FROM {{ ref('intermediate_users') }}),
        (SELECT MAX(kafka_ts) FROM {{ ref('intermediate_order_items') }})
    )
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY created_at DESC, kafka_ts DESC
        ) AS rn
    FROM changed_users
)
SELECT * EXCLUDE rn FROM ranked WHERE rn = 1
```

**Customer tiers:**
- **High-value:** `lifetime_value > 500 OR order_count > 10`
- **Repeat:** `order_count > 3 AND lifetime_value <= 500`
- **New:** `created_at` within last 90 days
- **Churned:** `last_order_date > 180 days ago`
- **Regular:** all others

### fct_sessions — 2-Hour Late-Arrival Window

Events from the same session can arrive in different micro-batches (30s intervals). The 2-hour window ensures accurate session metrics:

```sql
COUNT(*) FILTER (WHERE event_type = 'page_view')
    OVER (
        PARTITION BY session_id
        ORDER BY EPOCH_MILLISECONDS(event_time) / 1000
        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW
    ) AS session_page_views
```

## Testing

Column-level tests in `models/*/schema.yml`:

| Model | Test | Column |
|-------|------|--------|
| `staging_orders` | `not_null` | `id`, `user_id` |
| `staging_orders` | `accepted_values` | `status` |
| `staging_order_items` | `not_null` | `id`, `order_id`, `product_id`, `sale_price` |
| `staging_events` | `not_null` | `id`, `event_type`, `session_id` |
| `staging_users` | `unique`, `not_null` | `id`; `not_null` | `email` |
| `staging_products` | `unique`, `not_null` | `id`; `not_null` | `cost`, `retail_price` |

## Sources

dbt sources in `models/sources.yml` map to HMS-registered Delta tables:

```yaml
sources:
  - name: staging
    database: delta
    schema: staging
    loaded_at_field: kafka_ts
    freshness:
      warn_after: {count: 1, period: hour}
      error_after: {count: 6, period: hour}
    tables:
      - name: orders
      - name: order_items
      - name: events
      - name: users
      - name: products      # freshness: null (static)
      - name: dist_centers # freshness: null (static)
```

## Running dbt

```bash
# Run all models
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt

# Run + test
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt && \
  dbt test --project-dir /dbt --profiles-dir /dbt

# Run specific layer
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt \
  --select path:models/intermediate

# Full refresh (mart tables)
docker compose exec airflow-scheduler /opt/dbt_venv/bin/dbt run --project-dir /opt/airflow/dbt --profiles-dir /opt/airflow/dbt \
  --full-refresh --select path:models/mart
```
