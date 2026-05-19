# Airflow Operators & Callbacks

DAG template, operators, retry strategy, Google Chat notification

---

## DAG Template

```python
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
    dag_id="my_etl",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["etl"],
    on_success_callback=on_success_callback,
) as dag:

    task_a = BashOperator(
        task_id="task_a",
        bash_command=f"{RUN_CMD} --job my_job",
    )
```

---

## BashOperator

ใช้สำหรับ on-premise deploy (git pull):

```python
BashOperator(
    task_id="data_sync",
    bash_command=f"{RUN_CMD} --job data_sync",
)
```

**Kerberos job** — kinit ก่อนรัน:

```python
BashOperator(
    task_id="hdfs_upload",
    bash_command=(
        "kinit -kt /etc/security/keytabs/etl.keytab etl_user@REALM.COM && "
        f"{RUN_CMD} --job example_upload_hdfs"
    ),
)
```

---

## DockerOperator

ใช้สำหรับ deploy แบบ Docker image (Harbor):

```python
from airflow.providers.docker.operators.docker import DockerOperator

DockerOperator(
    task_id="data_sync",
    image="harbor.company.com/etl/etl-template:latest",
    command="--job data_sync",
    environment={
        "POSTGRES_HOST": "db.prod",
        "POSTGRES_PASSWORD": "{{ var.value.pg_password }}",
        "GCHAT_WEBHOOK_URL": "{{ var.value.gchat_webhook }}",
    },
    auto_remove=True,
    docker_url="unix:///var/run/docker.sock",
    network_mode="host",
    on_failure_callback=on_failure_callback,
    on_retry_callback=on_retry_callback,
)
```

**Kerberos + Docker** — mount keytab:

```python
DockerOperator(
    task_id="hdfs_upload",
    image="harbor.company.com/etl/etl-template:latest",
    command="--job example_upload_hdfs",
    environment={
        "HDFS_AUTH_MECHANISM": "GSSAPI",
        "HDFS_KERBEROS_PRINCIPAL": "etl_user@REALM.COM",
        "KRB5_CLIENT_KTNAME": "/app/user.keytab",
    },
    mounts=["/etc/security/keytabs/etl.keytab:/app/user.keytab:ro"],
    auto_remove=True,
)
```

---

## Google Chat Callbacks

`utils/notify.py` มี 3 callbacks:

| Callback | Trigger | Emoji | Scope |
|---|---|---|---|
| `on_failure_callback` | Task failed (retry หมดแล้ว) | 🔴 | `default_args` (ทุก task) |
| `on_retry_callback` | Task retry (ก่อน retry แต่ละครั้ง) | 🟡 | `default_args` (ทุก task) |
| `on_success_callback` | DAG สำเร็จทั้ง pipeline | 🟢 | DAG level |

**ข้อความตัวอย่าง:**

```
🔴 Task Failed
DAG: etl_example
Task: data_sync
Execution: 2025-06-01T02:00:00+00:00
Error: Command exited with return code 1
Log: http://airflow:8080/log?...
```

```
🟡 Task Retry
DAG: etl_example
Task: data_sync
Attempt: 2
Error: Connection refused
```

**ต้องตั้ง env:**
```bash
GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/XXX/messages?key=YYY&token=ZZZ
```

---

## Retry Strategy

```python
default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,       # 3m → 6m → 12m...
    "max_retry_delay": timedelta(minutes=30),
}
```

| Parameter | ค่าแนะนำ | หมายเหตุ |
|---|---|---|
| `retries` | 2-3 | ไม่ควรเกิน 3 — GChat noti ทุกรอบ |
| `retry_delay` | 3-5 min | ให้ DB/network recover |
| `retry_exponential_backoff` | True | เหมาะกับ transient errors |
| `max_retry_delay` | 30 min | cap ไม่ให้รอนานเกิน |

---

## Task Dependencies

```python
# Sequential
task_a >> task_b >> task_c

# Parallel then join
[task_a, task_b] >> task_c

# Conditional (BranchPythonOperator)
from airflow.operators.python import BranchPythonOperator

def choose_branch(**context):
    return "full_sync" if context["ds_nodash"][-2:] == "01" else "incremental"

branch = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
full_sync = BashOperator(task_id="full_sync", bash_command=f"{RUN_CMD} --job full_sync")
incremental = BashOperator(task_id="incremental", bash_command=f"{RUN_CMD} --job incremental")

branch >> [full_sync, incremental]
```

---

## SLA & Timeout

```python
BashOperator(
    task_id="data_sync",
    bash_command=f"{RUN_CMD} --job data_sync",
    execution_timeout=timedelta(hours=2),    # kill ถ้ารันเกิน 2 ชม.
    sla=timedelta(hours=1),                  # alert ถ้ายังไม่เสร็จใน 1 ชม.
)
```

---

## เลือก Operator ไหน?

| สถานการณ์ | Operator | เหตุผล |
|---|---|---|
| On-premise, debug ง่าย | `BashOperator` | แก้ code → git pull ได้เลย |
| Isolated environment, reproducible | `DockerOperator` | ไม่พึ่ง server deps |
| ต้องการ K8s scaling | `KubernetesPodOperator` | แต่ละ task เป็น Pod |
| Python-only logic (ไม่ต้อง CLI) | `PythonOperator` | import job class ตรง |
