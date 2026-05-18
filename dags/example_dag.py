"""Example Airflow DAG — calls main.py --job via BashOperator."""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="etl_example",
    schedule_interval="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl"],
) as dag:

    data_sync = BashOperator(
        task_id="data_sync",
        bash_command="cd /opt/airflow/dags/etl-template && uv run main.py --job data_sync",
    )

    backup = BashOperator(
        task_id="backup",
        bash_command="cd /opt/airflow/dags/etl-template && uv run main.py --job backup",
    )

    healthcheck = BashOperator(
        task_id="healthcheck",
        bash_command="cd /opt/airflow/dags/etl-template && uv run main.py --job healthcheck",
    )

    data_sync >> backup >> healthcheck
