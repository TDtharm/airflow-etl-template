"""Production DAG — Daily ETL: extract from source DB → transform → load to data warehouse.

Pattern: parallel extract → sequential transform+load → notification
Schedule: ทุกวัน 02:00 (หลัง source DB ปิดวัน)
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
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id="prod_daily_etl",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl", "production", "daily"],
    max_active_runs=1,
    on_success_callback=on_success_callback,
    doc_md="""
    ## Daily ETL Pipeline
    - **Source:** PostgreSQL (OLTP)
    - **Target:** Impala/Kudu (DWH)
    - **Schedule:** 02:00 daily
    - **SLA:** ต้องเสร็จก่อน 06:00
    """,
) as dag:

    # ────────────────────────────────────────────────
    # Extract (parallel — ดึงจากหลาย source พร้อมกัน)
    # ────────────────────────────────────────────────

    extract_users = BashOperator(
        task_id="extract_users",
        bash_command=f"{RUN_CMD} --job extract_users",
    )

    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command=f"{RUN_CMD} --job extract_orders",
    )

    extract_products = BashOperator(
        task_id="extract_products",
        bash_command=f"{RUN_CMD} --job extract_products",
    )

    # ────────────────────────────────────────────────
    # Transform + Load (sequential — dependency order)
    # ────────────────────────────────────────────────

    load_dim_users = BashOperator(
        task_id="load_dim_users",
        bash_command=f"{RUN_CMD} --job load_dim_users",
    )

    load_dim_products = BashOperator(
        task_id="load_dim_products",
        bash_command=f"{RUN_CMD} --job load_dim_products",
    )

    load_fact_orders = BashOperator(
        task_id="load_fact_orders",
        bash_command=f"{RUN_CMD} --job load_fact_orders",
        # fact table ขึ้นกับ dimension ทั้ง 2
    )

    # ────────────────────────────────────────────────
    # Post-load: data quality check
    # ────────────────────────────────────────────────

    dq_check = BashOperator(
        task_id="data_quality_check",
        bash_command=f"{RUN_CMD} --job data_quality_check",
        execution_timeout=timedelta(minutes=30),
    )

    # ────────────────────────────────────────────────
    # Dependencies
    # ────────────────────────────────────────────────

    # Extract parallel
    [extract_users, extract_orders, extract_products]

    # Dim load (parallel, ขึ้นกับ extract)
    extract_users >> load_dim_users
    extract_products >> load_dim_products

    # Fact load (ขึ้นกับ dim + extract orders)
    [load_dim_users, load_dim_products, extract_orders] >> load_fact_orders

    # DQ check หลัง load ทั้งหมด
    load_fact_orders >> dq_check
