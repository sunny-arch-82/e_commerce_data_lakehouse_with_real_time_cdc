# Apache Spark

Distributed data processing engine. In this lakehouse, Spark Structured Streaming is utilized to consume CDC JSON records from Kafka topics in micro-batches and write them to the Delta Lake storage layer (MinIO).

## Components

- **Spark Master:** Coordinates the cluster and resource allocation.
- **Spark Worker:** Executes the streaming tasks and writes to Delta.

## Configuration

- **Master Web UI:** http://localhost:8088
- **Worker Web UI:** http://localhost:8081

## Running

Spark is included in the core services.

```bash
make up-core
```
