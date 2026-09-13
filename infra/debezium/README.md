# Debezium

Change Data Capture (CDC) platform based on Kafka Connect. It reads the PostgreSQL Write-Ahead Log (WAL) using the `pgoutput` plugin and publishes row-level changes to Kafka topics.

## Configuration

- **Port:** `8083` (Kafka Connect REST API)
- **SMT (Single Message Transformations):** Uses Debezium `ExtractNewRecordState` to unwrap the CDC envelope and publishes Confluent Avro through Schema Registry.

Connectors are auto-provisioned from the configuration files upon container startup.

## Running

Debezium is part of the core services.

```bash
make up-core
```
