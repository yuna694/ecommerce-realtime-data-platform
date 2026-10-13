"""Batch orchestration: Silver (Delta) -> Gold (Spark) -> Warehouse -> dbt models + tests.

The streaming jobs (bronze/silver/realtime) run continuously as their own services.
Airflow owns what is *scheduled and dependent*:

    wait_for_silver >> spark_gold >> dbt_seed >> dbt_run >> dbt_test

Failures retry twice, then write a row to monitoring.alerts (visible in Power BI).
"""
import os
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

LAKE = os.getenv("LAKE_PATH", "/lake")
DBT = "/opt/dbt_venv/bin/dbt"
DBT_ARGS = "--project-dir /opt/project/dbt --profiles-dir /opt/project/dbt"


def _silver_ready() -> bool:
    return all(os.path.isdir(f"{LAKE}/silver/{t}/_delta_log") for t in ("orders", "payments", "clicks", "quarantine"))


def _alert_on_failure(context) -> None:
    ti = context["task_instance"]
    try:
        conn = psycopg2.connect(
            host=os.getenv("WAREHOUSE_HOST", "warehouse"), port=int(os.getenv("WAREHOUSE_PORT", "5432")),
            dbname=os.getenv("WAREHOUSE_DB", "warehouse"), user=os.getenv("WAREHOUSE_USER", "de"),
            password=os.getenv("WAREHOUSE_PASSWORD", "de_password"))
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO monitoring.alerts (source, severity, window_start, message) "
                "VALUES (%s, 'critical', now(), %s) ON CONFLICT DO NOTHING",
                (f"airflow:{ti.dag_id}.{ti.task_id}", f"Task failed after retries (try {ti.try_number}). Check Airflow logs."))
        conn.close()
    except Exception as exc:  # an alerting failure must never mask the original failure
        print(f"could not write alert: {exc}")


with DAG(
    dag_id="ecommerce_batch_pipeline",
    description="Silver -> Gold (Spark) -> dbt star schema + data tests",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",          # every 10 minutes (demo cadence; hourly in production)
    catchup=False,
    max_active_runs=1,                # never run two gold builds concurrently
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "on_failure_callback": _alert_on_failure,
    },
    tags=["ecommerce", "batch"],
) as dag:

    wait_for_silver = PythonSensor(
        task_id="wait_for_silver",
        python_callable=_silver_ready,
        poke_interval=15,
        timeout=15 * 60,
        mode="reschedule",            # frees the worker slot while waiting
    )

    spark_gold = BashOperator(
        task_id="spark_gold",
        bash_command="python /opt/project/spark_jobs/gold_batch.py",
        execution_timeout=timedelta(minutes=15),
    )

    dbt_seed = BashOperator(task_id="dbt_seed", bash_command=f"{DBT} seed {DBT_ARGS}")
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run {DBT_ARGS}")
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT} test {DBT_ARGS}",
        retries=0,                    # a failing data test is not transient: don't retry, alert
    )

    wait_for_silver >> spark_gold >> dbt_seed >> dbt_run >> dbt_test
