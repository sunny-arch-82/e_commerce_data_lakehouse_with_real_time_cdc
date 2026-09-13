"""E-Commerce CDC Lakehouse — hourly transformations and CDC freshness guard."""

from datetime import datetime
from pathlib import Path
import os

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

DBT_PROJECT_PATH = Path("/opt/airflow/dbt")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="ecommerce_cdc_lakehouse",
    install_dbt_deps=False,
)

profile_config = ProfileConfig(
    profile_name="ecommerce_trino",
    target_name="dev",
    profiles_yml_filepath=DBT_PROJECT_PATH / "profiles.yml",
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path="/opt/dbt_venv/bin/dbt",
)


def check_cdc_lag() -> None:
    """Fail the DAG when the recent CDC lag exceeds the configured threshold."""
    import trino

    threshold_ms = int(os.getenv("CDC_LAG_ALERT_THRESHOLD_MS", "300000"))
    conn = trino.dbapi.connect(host=os.getenv("TRINO_HOST", "trino"), port=8080, user="airflow", catalog="delta")
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT max(max_cdc_lag_ms)
            FROM monitoring.cdc_batch_metrics
            WHERE recorded_at >= current_timestamp - INTERVAL '2' HOUR
        """)
        max_lag = cur.fetchone()[0]
    except Exception as exc:
        print(f"CDC metrics are not available yet; continuing dbt pipeline: {exc}")
        return

    if max_lag is None:
        print("No CDC lag metrics were recorded in the last two hours; continuing dbt pipeline.")
        return
    if int(max_lag) > threshold_ms:
        raise AirflowException(f"CDC max lag {max_lag} ms exceeds threshold {threshold_ms} ms")


with DAG(
    dag_id="ecommerce_cdc_dbt_pipeline",
    description="Hourly CDC health check and dbt transformations — staging → intermediate → mart",
    schedule="0 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "airflow", "retries": 2},
    tags=["ecommerce", "cdc", "dbt", "lakehouse"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    cdc_health_check = PythonOperator(
        task_id="check_cdc_lag",
        python_callable=check_cdc_lag,
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && /opt/dbt_venv/bin/dbt deps",
    )

    common_render = dict(load_method=LoadMode.DBT_LS, test_behavior=TestBehavior.AFTER_EACH, dbt_deps=True)

    staging = DbtTaskGroup(
        group_id="staging",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/staging"], **common_render),
    )

    intermediate = DbtTaskGroup(
        group_id="intermediate",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/intermediate"], **common_render),
    )

    mart = DbtTaskGroup(
        group_id="mart",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(select=["path:models/mart"], **common_render),
    )

    start >> cdc_health_check >> dbt_deps >> staging >> intermediate >> mart >> end
