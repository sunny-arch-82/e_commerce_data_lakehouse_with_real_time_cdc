# Apache Kafka

Event streaming platform used to decouple the ingestion from PostgreSQL (via Debezium) and the downstream processing (Spark). This project runs Kafka in KRaft mode, eliminating the need for Zookeeper.

## Configuration

- **Port:** `9092` (Broker)
- **Mode:** KRaft
- **Topics:** Automatically created by Debezium when CDC events are published. They follow the `ecommerce.public.*` naming convention.

## Running

Kafka is part of the core infrastructure.

```bash
make up-core
```
