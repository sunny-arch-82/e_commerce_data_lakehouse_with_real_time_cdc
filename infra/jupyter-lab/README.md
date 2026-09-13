# JupyterLab

Interactive workspace for data engineering and data science. Includes PySpark and Delta Lake dependencies to query and manipulate data directly from MinIO.

## Features

- Pre-configured Spark Session with Delta Lake support.
- Configured to connect to the centralized Hive Metastore.
- Used to bootstrap initial tables and run interactive ad-hoc streaming workloads.

## Configuration

- **Port:** `8888` (Web UI)

## Running

```bash
make up-explore
```
