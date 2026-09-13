# Hive Metastore

Centralized metadata catalog that manages table definitions, schemas, and partition locations for Delta tables stored in MinIO.

## Configuration

- **Port:** `9083` (Thrift API)
- **Backend:** MariaDB (stores the metadata database)

Both Trino and JupyterLab (Spark) are configured to connect to this HMS instance to read and register Delta tables.

## Running

HMS is part of the core services.

```bash
make up-core
```
