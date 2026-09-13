# Airflow — CDC Health + dbt Orchestration

**DAG:** `infra/airflow/dags/ecommerce_cdc_pipeline_dag.py`

The hourly DAG combines an operational freshness guard with the analytics transformation chain:

```text
start
  -> check_cdc_lag
  -> dbt_deps
  -> staging models + tests
  -> intermediate models + tests
  -> mart models + tests
  -> end
```

## CDC health check

`check_cdc_lag` queries `delta.monitoring.cdc_batch_metrics` through Trino. If no metrics exist yet, the task is skipped. If the recent maximum lag is greater than `CDC_LAG_ALERT_THRESHOLD_MS` (default: 300000 ms / 5 minutes), the task fails so stale data does not silently flow through the transformation DAG.

## Cosmos/dbt configuration

Cosmos uses `LoadMode.DBT_LS` so the repository does not need to commit generated `target/manifest.json` files. dbt runs locally inside the Airflow container using `/opt/dbt_venv/bin/dbt`.

The dbt project and profile are:

```text
project: ecommerce_cdc_lakehouse
profile: ecommerce_trino
```

## Manual commands

Trigger the DAG:

```bash
docker compose exec airflow-webserver airflow dags trigger ecommerce_cdc_dbt_pipeline
```

List runs:

```bash
docker compose exec airflow-webserver airflow dags list-runs -d ecommerce_cdc_dbt_pipeline
```

Run dbt directly inside the Airflow scheduler container:

```bash
docker compose exec airflow-scheduler \
  /opt/dbt_venv/bin/dbt run \
  --project-dir /opt/airflow/dbt \
  --profiles-dir /opt/airflow/dbt
```

Run dbt tests:

```bash
make test
```
