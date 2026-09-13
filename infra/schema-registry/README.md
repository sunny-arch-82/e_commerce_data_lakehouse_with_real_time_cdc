# Schema Registry

Provides a RESTful interface for storing and retrieving Avro, JSON Schema, and Protobuf schemas. It ensures that the Kafka messages conform to the expected format and manages schema evolution.

## Configuration

- **Port:** `8081`

## Running

Schema Registry is started along with the core services.

```bash
make up-core
```
