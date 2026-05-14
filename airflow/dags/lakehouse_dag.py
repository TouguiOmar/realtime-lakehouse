"""
lakehouse_dag.py
────────────────
Airflow DAG that orchestrates the Silver and Gold writers
every 15 minutes.

Pipeline:
  silver_writer.py → gold_writer.py

The Bronze writer runs continuously as a standalone Spark
Structured Streaming job — Airflow does not manage it.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# ── Spark submit base command ─────────────────────────────────────
SPARK_SUBMIT = (
    "docker exec lakehouse-spark-master "
    "/opt/spark/bin/spark-submit "
    "--master local[2] "
    "--packages "
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262"
)

SILVER_JOB = f"{SPARK_SUBMIT} /opt/spark-apps/silver_writer.py"
GOLD_JOB   = f"{SPARK_SUBMIT} /opt/spark-apps/gold_writer.py"

# ── Default args ──────────────────────────────────────────────────
default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# ── DAG definition ────────────────────────────────────────────────
with DAG(
    dag_id="lakehouse_pipeline",
    description="Orchestrates Silver MERGE and Gold aggregations",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/15 * * * *",   # every 15 minutes
    catchup=False,
    tags=["lakehouse", "iceberg", "cdc"],
) as dag:

    start = DummyOperator(task_id="start")

    # ── Silver: deduplicate Bronze + MERGE upserts ────────────────
    run_silver = BashOperator(
        task_id="run_silver_writer",
        bash_command=SILVER_JOB,
        execution_timeout=timedelta(minutes=10),
    )

    # ── Gold: aggregate Silver into 3 business tables ─────────────
    run_gold = BashOperator(
        task_id="run_gold_writer",
        bash_command=GOLD_JOB,
        execution_timeout=timedelta(minutes=5),
    )

    end = DummyOperator(task_id="end")

    # ── Dependencies: Silver must succeed before Gold runs ────────
    start >> run_silver >> run_gold >> end