"""Example Airflow DAG — calls main.py --job via BashOperator with GChat notifications."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from utils.notify import on_failure_callback, on_retry_callback, on_success_callback

PROJECT_DIR = "/opt/airflow/dags/etl-template"
RUN_CMD = f"cd {PROJECT_DIR} && uv run main.py"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG(
    dag_id="etl_example",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl"],
    on_success_callback=on_success_callback,
) as dag:

    data_sync = BashOperator(
        task_id="data_sync",
        bash_command=f"{RUN_CMD} --job data_sync",
    )

    backup = BashOperator(
        task_id="backup",
        bash_command=f"{RUN_CMD} --job backup",
    )

    healthcheck = BashOperator(
        task_id="healthcheck",
        bash_command=f"{RUN_CMD} --job healthcheck",
    )

    data_sync >> backup >> healthcheck
