"""Production DAG — Weekly full snapshot + archive to HDFS/Iceberg.

Pattern: Full export → Parquet → HDFS archive + Iceberg table refresh
Schedule: ทุกวันอาทิตย์ 01:00
Use case: Historical backup, data lake, ML training data
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import os

from utils.notify import on_failure_callback, on_retry_callback, on_success_callback

PROJECT_DIR = os.environ.get("ETL_PROJECT_DIR", "/opt/airflow/dags/etl-template")
RUN_CMD = f"cd {PROJECT_DIR} && uv run main.py"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=4),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id="prod_weekly_archive",
    default_args=default_args,
    schedule="0 1 * * 0",  # ทุกวันอาทิตย์ 01:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "production", "weekly", "archive"],
    max_active_runs=1,
    on_success_callback=on_success_callback,
    doc_md="""
    ## Weekly Full Archive
    - **Source:** Kudu (DWH)
    - **Target:** HDFS Parquet + Iceberg
    - **Path:** /data/archive/{table}/{year}/{month}/{day}/
    """,
) as dag:

    # ────────────────────────────────────────────────
    # Export full snapshot to Parquet on HDFS
    # ────────────────────────────────────────────────

    archive_orders = BashOperator(
        task_id="archive_orders_hdfs",
        bash_command=f"{RUN_CMD} --job archive_orders_to_hdfs",
    )

    archive_users = BashOperator(
        task_id="archive_users_hdfs",
        bash_command=f"{RUN_CMD} --job archive_users_to_hdfs",
    )

    # ────────────────────────────────────────────────
    # Refresh Iceberg table (MERGE INTO from archive)
    # ────────────────────────────────────────────────

    refresh_iceberg = BashOperator(
        task_id="refresh_iceberg_tables",
        bash_command=f"{RUN_CMD} --job refresh_iceberg_weekly",
        execution_timeout=timedelta(hours=2),
    )

    # ────────────────────────────────────────────────
    # Cleanup old archives (retention: 90 days)
    # ────────────────────────────────────────────────

    cleanup = BashOperator(
        task_id="cleanup_old_archives",
        bash_command=f"{RUN_CMD} --job cleanup_hdfs_archives --retention-days 90",
        execution_timeout=timedelta(minutes=30),
    )

    # Archive parallel → Iceberg refresh → cleanup
    [archive_orders, archive_users] >> refresh_iceberg >> cleanup
