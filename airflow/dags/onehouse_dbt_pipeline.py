from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor


DBT_PROJECT_DIR = "/opt/airflow/dbt_onehouse"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"
DBT_LOG_DIR = "/opt/airflow/logs/dbt"
DBT_TARGET_DIR = "/opt/airflow/dbt_target"
DBT_BIN = "/home/airflow/.local/bin/dbt"


def trino_is_ready() -> bool:
    import trino

    host = os.getenv("TRINO_HOST", "trino")
    port = int(os.getenv("TRINO_PORT", "8080"))
    user = os.getenv("TRINO_USER", "admin")

    try:
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog="iceberg",
            schema="gold",
            http_scheme="http",
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        return True
    except Exception as exc:  # Airflow logs the exception details on each poke.
        print(f"Trino is not ready yet: {exc}")
        return False


DEFAULT_ENV = {
    "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
    "DBT_TARGET_PATH": DBT_TARGET_DIR,
    "TRINO_HOST": os.getenv("TRINO_HOST", "trino"),
    "TRINO_PORT": os.getenv("TRINO_PORT", "8080"),
    "TRINO_USER": os.getenv("TRINO_USER", "admin"),
}


def dbt_cmd(command: str) -> str:
    return (
        f"mkdir -p {DBT_LOG_DIR} {DBT_TARGET_DIR} && "
        f"cd {DBT_PROJECT_DIR} && "
        f"{DBT_BIN} --log-path {DBT_LOG_DIR} "
        f"{command} --profiles-dir {DBT_PROFILES_DIR} --project-dir {DBT_PROJECT_DIR}"
    )


with DAG(
    dag_id="onehouse_dbt_medallion_pipeline",
    description="Run OneHouse dbt ELT and data quality gates on Trino/Iceberg.",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "onehouse",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["onehouse", "dbt", "trino", "iceberg"],
) as dag:
    wait_for_trino = PythonSensor(
        task_id="wait_for_trino",
        python_callable=trino_is_ready,
        poke_interval=20,
        timeout=300,
        mode="poke",
    )

    debug_profile = BashOperator(
        task_id="dbt_debug",
        bash_command=dbt_cmd("debug"),
        env=DEFAULT_ENV,
    )

    seed_reference_data = BashOperator(
        task_id="dbt_seed_bts_metadata",
        bash_command=dbt_cmd("seed --select bts_metadata --full-refresh"),
        env=DEFAULT_ENV,
    )

    run_staging_and_silver = BashOperator(
        task_id="dbt_run_staging_silver",
        bash_command=dbt_cmd("run --select staging silver"),
        env=DEFAULT_ENV,
    )

    run_gold_incremental = BashOperator(
        task_id="dbt_run_gold_incremental",
        bash_command=dbt_cmd("run --select gold"),
        env=DEFAULT_ENV,
    )

    quality_gate_gold = BashOperator(
        task_id="dbt_test_gold_quality_gate",
        bash_command=dbt_cmd("test --select tag:gold"),
        env=DEFAULT_ENV,
    )

    generate_lineage_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=dbt_cmd("docs generate"),
        env=DEFAULT_ENV,
    )

    (
        wait_for_trino
        >> debug_profile
        >> seed_reference_data
        >> run_staging_and_silver
        >> run_gold_incremental
        >> quality_gate_gold
        >> generate_lineage_docs
    )
