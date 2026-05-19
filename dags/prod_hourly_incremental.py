"""Production DAG — Hourly incremental sync (CDC-style).

Pattern: ดึงเฉพาะ data ที่เปลี่ยนแปลงใน 1 ชม.ล่าสุด → upsert เข้า DWH
Schedule: ทุกชั่วโมง (offset 10 นาที เพื่อรอ source commit)
Use case: Near real-time dashboard, ไม่ต้องรอ daily batch
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import os

from utils.notify import on_failure_callback, on_retry_callback

PROJECT_DIR = os.environ.get("ETL_PROJECT_DIR", "/opt/airflow/dags/etl-template")
RUN_CMD = f"cd {PROJECT_DIR} && uv run main.py"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id="prod_hourly_incremental",
    default_args=default_args,
    schedule="10 * * * *",  # ทุกชั่วโมง นาทีที่ 10
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "production", "hourly", "incremental"],
    max_active_runs=1,  # ป้องกัน overlap (ถ้า run ก่อนหน้ายังไม่เสร็จ)
    dagrun_timeout=timedelta(minutes=50),  # timeout ทั้ง DAG run
    doc_md="""
    ## Hourly Incremental Sync
    - **Source:** PostgreSQL (modified_at > last_run)
    - **Target:** Kudu (UPSERT INTO)
    - **Strategy:** Incremental — ดึงเฉพาะ rows ที่ modified_at > execution_date - 1h
    """,
) as dag:

    sync_transactions = BashOperator(
        task_id="sync_transactions",
        bash_command=f"{RUN_CMD} --job sync_transactions_incremental",
    )

    sync_inventory = BashOperator(
        task_id="sync_inventory",
        bash_command=f"{RUN_CMD} --job sync_inventory_incremental",
    )

    # parallel — ไม่มี dependency ระหว่าง table
    [sync_transactions, sync_inventory]
