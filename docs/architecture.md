# Architecture

## System Overview

E-Commerce CDC Lakehouse is a **streaming data lakehouse** that ingests e-commerce events from PostgreSQL via Debezium CDC into Kafka, processes them through Spark Structured Streaming onto Delta Lake (MinIO), and serves business-ready metrics through Trino and Superset — all orchestrated by Airflow and dbt.

## Data Flow

```mermaid
sequenceDiagram
    participant PG as PostgreSQL
    participant D as Debezium
    participant K as Kafka
    participant S as Spark Notebook
    participant M as MinIO
    participant H as Hive Metastore
    participant T as Trino
    participant A as Airflow + dbt

    PG->>D: WAL (pgoutput)
    D->>K: Confluent Avro (SMT-unwrapped row + CDC metadata)
    K->>S: Kafka topics
    S->>M: Delta Lake (s3a://lakehouse/staging/)
    M->>H: (no auto-discovery)
    S->>H: Register existing Delta tables via Trino system.register_table
    H->>T: Table metadata (schema, location)
    T->>M: Read/write via HMS
    A->>T: dbt transformations
    A->>M: Write intermediate/mart
    T->>A: Query results
```

## Three-Layer Architecture (Medallion)

```mermaid
flowchart TB
    subgraph staging["Bronze · staging/"]
        S1[orders]
        S2[order_items]
        S3[events]
        S4[users]
        S5[products]
        S6[dist_centers]
    end
    subgraph intermediate["Silver · intermediate/"]
        I1[intermediate_orders]
        I2[intermediate_order_items]
        I3[intermediate_events]
        I4[intermediate_users]
        I5[intermediate_products]
    end
    subgraph mart["Gold · mart/"]
        D1[dim_customers]
        D2[dim_products]
        D3[dim_date]
        F1[fct_orders]
        F2[fct_order_items]
        F3[fct_events]
        F4[fct_sessions]
    end

    staging --> intermediate --> mart
```

### Layer Responsibilities

| Layer | dbt materialization | Path | Responsibility |
|-------|---------------------|------|----------------|
| **staging/** | `ephemeral` | `s3a://lakehouse/staging/` | Raw typed CDC, no dedup |
| **intermediate/** | `incremental` | `s3a://lakehouse/intermediate/` | Deduplication + enrichment |
| **mart/** | `table` | `s3a://lakehouse/mart/` | Star schema dims + facts |

## Docker Services

| Service | Role | Key Config |
|---------|------|------------|
| `postgres` | OLTP source + CDC WAL | `wal_level=logical`, pgoutput plugin |
| `kafka` | Message broker | KRaft mode, topics: `ecommerce.public.*` |
| `debezium` | CDC connector | `ExtractNewRecordState` SMT + Confluent Avro converters |
| `spark-master` / `spark-worker` | Cluster mode | Spark 3.5 |
| `jupyter-lab` | Notebook runtime | pyspark-notebook 3.5 + Hive 4.1.0 client + Delta 3.0 |
| `hive-metastore` | Metadata registry | HMS 4.1.0 + MariaDB backend |
| `trino` | Query engine | Delta Lake connector |
| `minio` | Object storage | S3-compatible |
| `data-generator` | Writes to PostgreSQL | Python 3.11 |
| `airflow-scheduler` / `airflow-webserver` | Orchestration | Airflow 3.0 + Cosmos |
| `superset` | BI dashboards | Apache Superset 3.1.3 |

All services communicate over `data_network` using Docker DNS names.

## Key Design Decisions

### 1. HMS Must Be Explicitly Registered

Hive Metastore is a **metadata registry**, not a discovery engine. Delta files written by Spark to MinIO must be registered before Trino/dbt can query them. The stream processor uses Trino's Delta procedure after writing each staging/monitoring/quarantine location:

```sql
CALL delta.system.register_table(
  schema_name => 'staging',
  table_name => 'events',
  table_location => 's3a://lakehouse/staging/events'
);
```

dbt creates and manages the `intermediate` and `mart` relations after the staging locations are visible through Trino/Hive Metastore.

> **Why?** Creating a new table definition over a location that already contains a Delta transaction log is rejected by Trino. Registering the existing Delta location preserves the Spark-written table and adds the required metastore metadata.

### 2. Two-Layer CDC Deduplication

Deduplication happens in two stages:

1. **Staging → Intermediate:** `ROW_NUMBER() OVER (PARTITION BY id ORDER BY kafka_ts DESC) = 1` — keeps latest record per entity
2. **Intermediate:** `delete+insert` on `unique_key` — replaces old records with new ones incrementally

### 3. Timestamp Strategy

| Field | Source | Usage |
|-------|--------|-------|
| `kafka_ts` | Kafka message metadata | Watermark for dedup + incremental filtering |
| `created_at`, `shipped_at`, etc. | ISO-8601 strings from Debezium | Cast with `CAST(col AS TIMESTAMP)` in mart |

### 4. Ghost Events

Events with `user_id IS NULL` (anonymous browsing) get `is_ghost=true` in `intermediate_events`. These are excluded from session aggregations but tracked in raw event counts.

