# E-Commerce Data Lakehouse with Real-Time CDC

A local, end-to-end data engineering platform that captures e-commerce changes from PostgreSQL with Debezium, streams them through Kafka and Spark Structured Streaming, stores curated data in Delta Lake on MinIO, transforms it with dbt, and orchestrates analytics workflows with Airflow.

This portfolio version extends the base lakehouse with an operational reliability layer around the CDC path: **schema-drift detection, bad-record quarantine, CDC lag metrics, and audited replay of quarantined events**.

## Architecture

```text
Synthetic E-Commerce Generator
          |
          v
     PostgreSQL 15
          |
          | WAL / logical replication
          v
       Debezium
          |
          v
   Kafka + Schema Registry
          |
          v
 Spark Structured Streaming
      /         |          \
     /          |           \
valid CDC   bad records   lag/schema metrics
   |             |             |
   v             v             v
Delta staging  Delta quarantine  Delta monitoring
   |
   v
 dbt (staging -> intermediate -> mart)
   |
   v
Trino -> Superset
   ^
   |
Airflow orchestration + CDC health check
```

## Portfolio Reliability Extensions

### 1. Schema-drift detection

Every Confluent Avro message carries a schema ID in its wire header. The Spark processor:

- reads that schema ID before decoding the record;
- compares the current Schema Registry schema with the local `.avsc` contract;
- detects added, removed, or type-changed business fields;
- quarantines events produced with an unapproved schema ID instead of letting them break the stream;
- writes drift details to `monitoring.schema_drift_events`.

### 2. Bad-record quarantine

Records are no longer silently discarded when their primary key is missing or the Avro payload cannot be decoded. Invalid events are written to the Delta quarantine table with:

- original topic, partition, and offset;
- original Kafka key/value as Base64;
- schema ID;
- quarantine reason;
- quarantine timestamp;
- a stable `source_event_id` (`topic:partition:offset`).

Quarantine reasons include `SCHEMA_DRIFT_UNAPPROVED`, `AVRO_DECODE_ERROR`, `MISSING_PRIMARY_KEY`, and `INVALID_CDC_OPERATION`.

### 3. CDC lag monitoring

Each Spark micro-batch records:

- processed record count;
- quarantined record count;
- average CDC lag;
- maximum CDC lag;
- latest Kafka offset.

Metrics are stored in `monitoring.cdc_batch_metrics`. The Airflow DAG performs a CDC health check and can fail when recent maximum lag exceeds `CDC_LAG_ALERT_THRESHOLD_MS`.

### 4. Safe replay

`workspace/replay_quarantine.py` can republish quarantined Kafka records after the underlying schema/data issue has been corrected. It is a dry run by default and writes successful replay IDs to a Delta audit table to avoid replaying the same original event repeatedly.

## Core Pipeline

1. The generator creates e-commerce users, orders, order items, events, products, and distribution-center data in PostgreSQL.
2. Debezium reads PostgreSQL WAL changes and publishes CDC events to `ecommerce.public.*` Kafka topics.
3. Schema Registry stores the Avro value schemas.
4. Spark Structured Streaming validates schema IDs, decodes approved events, quarantines invalid events, measures lag, and merges current entity state into Delta staging tables.
5. dbt builds staging, intermediate, and mart models on Delta through Trino.
6. Airflow orchestrates hourly dbt transformations and checks CDC freshness.
7. Superset queries the mart layer through Trino.

## Tech Stack

| Layer | Technology |
| --- | --- |
| Source database | PostgreSQL 15 |
| Change Data Capture | Debezium 3.0 |
| Streaming broker | Apache Kafka (KRaft) |
| Schema management | Confluent Schema Registry |
| Stream processing | Apache Spark 3.5 Structured Streaming |
| Lakehouse format | Delta Lake 3.0 |
| Object storage | MinIO |
| Metadata | Hive Metastore |
| SQL query engine | Trino |
| Transformations | dbt + dbt-trino |
| Orchestration | Apache Airflow + Astronomer Cosmos |
| BI | Apache Superset |

## Quick Start

### Prerequisites

- Docker Engine
- Docker Compose v2
- Make
- 16 GB RAM recommended for the complete stack

### 1. Configure local environment

```bash
cp .env.example .env
```

The included values are local-development defaults. Change passwords/secrets before using the project outside a local environment.

### 2. Start the core services

```bash
make up-core
```

Check them with:

```bash
make ps
```

### 3. Start the data generator

```bash
make up-datagen
```

This creates the source tables and begins producing PostgreSQL changes for Debezium.

### 4. Start Spark/Jupyter and the CDC processor

```bash
make up-jupyter
make run-stream
```

The processor runs continuously with a 30-second trigger interval. Keep this command running in its terminal while using a second terminal for Airflow, Trino, or dbt commands.

### 5. Start Airflow/dbt

```bash
make up-airflow
```

Airflow: `http://localhost:8085`

### 6. Optional: start the full stack

```bash
make up-all
```

## Monitoring Queries

Recent CDC lag:

```sql
SELECT
    table_name,
    max(max_cdc_lag_ms) AS max_lag_ms,
    avg(avg_cdc_lag_ms) AS avg_lag_ms,
    sum(quarantined_records) AS quarantined
FROM delta.monitoring.cdc_batch_metrics
WHERE recorded_at >= current_timestamp - INTERVAL '1' HOUR
GROUP BY table_name
ORDER BY max_lag_ms DESC;
```

Recent quarantined records:

```sql
SELECT table_name, quarantine_reason, count(*) AS records
FROM delta.quarantine.cdc_bad_records
GROUP BY table_name, quarantine_reason
ORDER BY records DESC;
```

Detected schema drift:

```sql
SELECT *
FROM delta.monitoring.schema_drift_events
ORDER BY detected_at DESC;
```

## Replay Quarantined Events

Preview pending records first:

```bash
make replay-quarantine TABLE=orders LIMIT=25
```

After correcting the cause of the failures:

```bash
make replay-quarantine TABLE=orders LIMIT=25 EXECUTE=1
```

Do not replay `SCHEMA_DRIFT_UNAPPROVED` events until the local schema contract and downstream Delta/dbt models have been intentionally updated.

## Example Analytics

After the dbt mart layer is built, Trino/Superset can query business-ready outputs. For example, orders by acquisition channel:

```sql
SELECT
    traffic_source,
    COUNT(*) AS total_orders
FROM mart.fct_orders
GROUP BY traffic_source
ORDER BY total_orders DESC;
```

This result can be visualized directly as a bar chart in Superset. The mart layer also supports customer segmentation, product revenue/margin analysis, and session-funnel analytics.

## Main Service Endpoints

| Service | Endpoint |
| --- | --- |
| Trino | `http://localhost:8080` |
| Schema Registry | `http://localhost:8081` |
| Debezium Connect | `http://localhost:8083` |
| Airflow | `http://localhost:8085` |
| Spark Master UI | `http://localhost:8088` |
| Superset | `http://localhost:8089` |
| MinIO Console | `http://localhost:9001` |
| JupyterLab | `http://localhost:8888` |

## Project Structure

```text
.
├── docker-compose.yaml
├── Makefile
├── .env.example
├── docs/
│   ├── architecture.md
│   ├── datasource.md
│   ├── datamodel.md
│   └── reliability.md
├── infra/
│   ├── airflow/
│   ├── data-generator/
│   ├── dbt/
│   ├── debezium/
│   ├── hive-metastore/
│   ├── jupyter-lab/
│   ├── kafka/
│   ├── schema-registry/
│   ├── spark/
│   ├── superset/
│   └── trino/
└── workspace/
    ├── stream_processor.py
    ├── stream_processor.ipynb
    └── replay_quarantine.py
```

## Project Lineage and Portfolio Scope

The engineering work specific to this version focuses on:

- the `ecommerce.*` service/topic namespace and project reorganization;
- Schema Registry-aware schema-drift detection;
- raw bad-record quarantine with replayable Kafka metadata;
- CDC lag and replay monitoring;
- Airflow freshness checks;
- reliable Delta table registration in Trino/Hive Metastore;
- startup/reproducibility fixes for Debezium, Airflow, dbt, and Docker Compose.




> The `datagen`, `explore`, and `airflow` Make targets also enable the `core` Compose profile because they rely on core services such as PostgreSQL, Kafka, Spark, MinIO, and Trino.
