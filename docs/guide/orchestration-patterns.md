# Orchestration Patterns — Airflow Workflow Control

รูปแบบการจัดการ pipeline: trigger, condition, dependency, dynamic generation

---

## Sensors — รอจน condition เป็น True

### FileSensor (รอ file มา)

```python
from airflow.sensors.filesystem import FileSensor

wait_for_file = FileSensor(
    task_id="wait_for_export",
    filepath="/data/incoming/sales_{{ ds }}.csv",
    poke_interval=60,        # check ทุก 60 วินาที
    timeout=3600,            # timeout 1 ชม.
    mode="poke",             # poke = hold worker slot ระหว่างรอ
)

process = BashOperator(task_id="process", bash_command=f"{RUN} --job load_sales")

wait_for_file >> process
```

### ExternalTaskSensor (รอ DAG อื่นเสร็จ)

```python
from airflow.sensors.external_task import ExternalTaskSensor

wait_for_extract = ExternalTaskSensor(
    task_id="wait_for_extract_dag",
    external_dag_id="daily_extract",
    external_task_id="final_task",       # รอ task นี้เสร็จ
    execution_date_fn=lambda dt: dt,     # same execution_date
    poke_interval=120,
    timeout=7200,
    mode="reschedule",                   # ← ไม่ hold worker slot
)
```

### SqlSensor (รอ data ใน DB)

```python
from airflow.providers.common.sql.sensors.sql import SqlSensor

wait_for_data = SqlSensor(
    task_id="wait_for_partition",
    conn_id="postgres_default",
    sql="""
        SELECT COUNT(*) FROM raw.sales
        WHERE sale_date = '{{ ds }}'
        HAVING COUNT(*) > 0
    """,
    poke_interval=300,
    timeout=7200,
    mode="reschedule",
)
```

### Mode: poke vs reschedule vs deferrable

| Mode | Behavior | Worker Slot | ใช้เมื่อ |
|---|---|---|---|
| `poke` | Loop check ใน worker | ❌ Hold ตลอด | รอสั้นๆ < 5 นาที |
| `reschedule` | Release slot ระหว่าง wait | ✅ คืน slot | รอนาน > 5 นาที |
| `deferrable` | Yield to triggerer (Airflow 2.6+) | ✅ คืน slot + lightweight | รอนานมาก, scale |

```python
# Deferrable sensor (Airflow 2.6+) — ไม่ใช้ worker slot เลย
from airflow.sensors.filesystem import FileSensor

wait = FileSensor(
    task_id="wait_file",
    filepath="/data/input.csv",
    deferrable=True,         # ← ใช้ triggerer แทน worker
    poke_interval=60,
)
```

---

## Trigger DAG — Chain หลาย DAG

### TriggerDagRunOperator

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# DAG A trigger DAG B เมื่อ extract เสร็จ
trigger_load = TriggerDagRunOperator(
    task_id="trigger_load_dag",
    trigger_dag_id="daily_load",                   # target DAG
    conf={"source": "sales", "date": "{{ ds }}"},  # ส่ง config ไป
    execution_date="{{ execution_date }}",         # same logical date
    wait_for_completion=False,                     # fire-and-forget
    reset_dag_run=True,                            # rerun ถ้ามี dagrun อยู่แล้ว
)

extract >> trigger_load
```

### Pattern: Extract DAG → Load DAG → Report DAG

```python
# dag_extract.py
with DAG("extract_all", schedule="0 5 * * *", ...):
    extract_users >> extract_orders >> extract_products >> trigger_load

# dag_load.py (triggered by extract_all)
with DAG("load_all", schedule=None, ...):  # schedule=None = triggered only
    load_dims >> load_facts >> trigger_report

# dag_report.py (triggered by load_all)
with DAG("daily_report", schedule=None, ...):
    build_report >> send_email
```

**ข้อดี:**
- DAG เล็ก, อ่านง่าย
- Retry scope เล็กลง (retry เฉพาะ stage ที่ fail)
- Team ต่างกันดูแลคนละ DAG

### wait_for_completion=True

```python
# รอ triggered DAG เสร็จ ก่อนไปต่อ
trigger = TriggerDagRunOperator(
    task_id="trigger_and_wait",
    trigger_dag_id="load_all",
    wait_for_completion=True,       # ← block จน target DAG เสร็จ
    poke_interval=60,
    allowed_states=["success"],     # fail ถ้า target DAG ไม่ success
    failed_states=["failed"],
)

trigger >> next_task
```

---

## Dataset-aware Scheduling (Airflow 2.4+)

Event-driven — DAG run เมื่อ dataset ถูก update (ไม่ต้อง cron, ไม่ต้อง sensor)

### Producer DAG (update dataset)

```python
from airflow.datasets import Dataset

# ประกาศ dataset
sales_dataset = Dataset("postgres://dwh/raw.sales")

with DAG("extract_sales", schedule="0 6 * * *", ...):
    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_sales_fn,
        outlets=[sales_dataset],      # ← ประกาศว่า task นี้ update dataset นี้
    )
```

### Consumer DAG (triggered by dataset update)

```python
from airflow.datasets import Dataset

sales_dataset = Dataset("postgres://dwh/raw.sales")
customers_dataset = Dataset("postgres://dwh/raw.customers")

# DAG นี้จะ run เมื่อ BOTH datasets ถูก update
with DAG(
    "build_mart",
    schedule=[sales_dataset, customers_dataset],  # ← event-driven!
    ...
):
    build = PythonOperator(task_id="build_mart", python_callable=build_fn)
```

### Flow

```
extract_sales (producer)                     extract_customers (producer)
    │ outlets=[sales_dataset]                     │ outlets=[customers_dataset]
    ▼                                             ▼
┌─────────────────────────────────────────────────────┐
│           Airflow Dataset Manager                     │
│   sales_dataset ✅  +  customers_dataset ✅          │
│   → trigger build_mart DAG                           │
└─────────────────────────────────────────────────────┘
    │
    ▼
build_mart (consumer) — runs automatically
```

**ข้อดี:**
- ไม่ต้อง Sensor (ไม่เปลือง worker slot)
- ไม่ต้อง TriggerDagRunOperator
- DAG decoupled — producer ไม่ต้องรู้จัก consumer
- UI แสดง dataset lineage

**ข้อจำกัด:**
- Dataset URI เป็น string — ไม่ validate ว่า data จริงๆ มีหรือไม่
- Consumer รอ ALL datasets update (AND logic, ไม่มี OR)

---

## Dynamic DAG Generation

### Pattern 1: Loop สร้าง Tasks (Static Config)

```python
TABLES = ["users", "orders", "products", "payments", "inventory"]

with DAG("sync_all_tables", schedule="0 6 * * *", ...):
    tasks = []
    for table in TABLES:
        task = BashOperator(
            task_id=f"sync_{table}",
            bash_command=f"{RUN} --job sync_table --table {table}",
            pool="source_db_pool",
        )
        tasks.append(task)

    # ทุก table ขนานกัน → validate
    validate = BashOperator(task_id="validate", bash_command=f"{RUN} --job validate_all")
    tasks >> validate
```

### Pattern 2: Config File → DAGs

```yaml
# config/pipelines.yaml
pipelines:
  - name: sync_users
    source: mssql
    target: postgres
    table: users
    schedule: "0 6 * * *"
    pool: source_db_pool

  - name: sync_orders
    source: mssql
    target: postgres
    table: orders
    schedule: "0 */2 * * *"
    pool: source_db_pool
```

```python
# dags/dynamic_pipelines.py
import yaml
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

config_path = Path(__file__).parent.parent / "config" / "pipelines.yaml"
config = yaml.safe_load(config_path.read_text())

for pipeline in config["pipelines"]:
    dag_id = f"auto_{pipeline['name']}"

    with DAG(
        dag_id=dag_id,
        schedule=pipeline["schedule"],
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["auto-generated", pipeline["source"]],
    ) as dag:
        BashOperator(
            task_id="sync",
            bash_command=f"{RUN} --job sync_table --table {pipeline['table']}",
            pool=pipeline.get("pool", "default_pool"),
        )

    globals()[dag_id] = dag  # ← register DAG กับ Airflow
```

**⚠️ Warning:** Config file ถูกอ่านทุก parse cycle — ใช้ file เล็ก, ไม่ query DB

### Pattern 3: DAG Factory (Class-based)

```python
# utils/dag_factory.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def create_sync_dag(table: str, schedule: str, pool: str = "default_pool") -> DAG:
    dag = DAG(
        dag_id=f"sync_{table}",
        schedule=schedule,
        start_date=datetime(2025, 1, 1),
        catchup=False,
        default_args={
            "retries": 2,
            "retry_delay": timedelta(minutes=5),
        },
        tags=["sync", table],
    )

    with dag:
        BashOperator(
            task_id="extract_load",
            bash_command=f"{RUN} --job sync_table --table {table}",
            pool=pool,
        )

    return dag

# dags/sync_tables.py
from utils.dag_factory import create_sync_dag

TABLES = {
    "users": {"schedule": "0 6 * * *", "pool": "source_db_pool"},
    "orders": {"schedule": "0 */2 * * *", "pool": "source_db_pool"},
    "products": {"schedule": "0 6 * * 1", "pool": "source_db_pool"},
}

for table, conf in TABLES.items():
    globals()[f"sync_{table}"] = create_sync_dag(table, **conf)
```

---

## Branching — เลือก Path ตาม Condition

### BranchPythonOperator

```python
from airflow.operators.python import BranchPythonOperator

def choose_path(**context):
    exec_date = context["execution_date"]
    if exec_date.weekday() < 5:  # Mon-Fri
        return "weekday_etl"
    else:
        return "weekend_cleanup"

branch = BranchPythonOperator(
    task_id="branch_weekday",
    python_callable=choose_path,
)

weekday_task = BashOperator(task_id="weekday_etl", bash_command=f"{RUN} --job daily_etl")
weekend_task = BashOperator(task_id="weekend_cleanup", bash_command=f"{RUN} --job cleanup")
join = BashOperator(task_id="notify", bash_command="echo done", trigger_rule="none_failed_min_one_success")

branch >> [weekday_task, weekend_task] >> join
```

### ShortCircuitOperator (skip downstream ถ้า False)

```python
from airflow.operators.python import ShortCircuitOperator

def check_has_data(**context):
    """Return True ถ้ามี data ใหม่, False ถ้าไม่มี → skip ทั้ง downstream"""
    import pandas as pd
    df = pd.read_sql(f"SELECT 1 FROM source WHERE dt = '{context['ds']}' LIMIT 1", conn)
    return len(df) > 0

check = ShortCircuitOperator(
    task_id="check_new_data",
    python_callable=check_has_data,
)

# ถ้า check return False → transform + load ถูก skip อัตโนมัติ
check >> transform >> load
```

---

## Trigger Rules — ควบคุม task execution

| Rule | Behavior |
|---|---|
| `all_success` (default) | Run เมื่อ upstream ทุกตัว success |
| `all_failed` | Run เมื่อ upstream ทุกตัว fail |
| `one_success` | Run เมื่อ upstream อย่างน้อย 1 ตัว success |
| `one_failed` | Run เมื่อ upstream อย่างน้อย 1 ตัว fail |
| `none_failed` | Run เมื่อไม่มี upstream ตัวไหน fail (success/skipped OK) |
| `none_failed_min_one_success` | เหมือน none_failed + ต้องมีอย่างน้อย 1 success |
| `all_done` | Run เมื่อ upstream ทุกตัวจบ (ไม่สน status) |

### ตัวอย่าง: Alert เมื่อ task ใดก็ตาม fail

```python
alert = PythonOperator(
    task_id="alert_on_failure",
    python_callable=send_alert,
    trigger_rule="one_failed",        # ← run ถ้ามี upstream fail
)

[extract, transform, load] >> alert
```

### ตัวอย่าง: Cleanup ทำเสมอ ไม่ว่าจะ success/fail

```python
cleanup = BashOperator(
    task_id="cleanup_temp",
    bash_command="rm -rf /tmp/etl_staging/*",
    trigger_rule="all_done",          # ← run เสมอ
)

extract >> transform >> load >> cleanup
```

---

## TaskFlow API (Airflow 2.x — Pythonic)

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="0 6 * * *", start_date=datetime(2025, 1, 1), catchup=False)
def daily_sales_taskflow():

    @task
    def extract():
        import pandas as pd
        df = pd.read_sql("SELECT * FROM source WHERE dt = '{{ ds }}'", conn)
        return df.to_dict()  # XCom (ใช้กับ data เล็กเท่านั้น!)

    @task
    def transform(data: dict):
        import pandas as pd
        df = pd.DataFrame(data)
        df["amount_thb"] = df["amount"] * 35.0
        return df.to_dict()

    @task
    def load(data: dict):
        import pandas as pd
        from utils.upsert import upsert_postgres
        df = pd.DataFrame(data)
        upsert_postgres(conn, df, "target", conflict_columns=["id"])

    # Dependency = function call chain
    raw = extract()
    transformed = transform(raw)
    load(transformed)

daily_sales_taskflow()
```

**ข้อดี:** อ่านง่ายเหมือน Python ปกติ, XCom implicit
**ข้อเสีย:** XCom overhead (serialize/deserialize data), ไม่เหมาะกับ data ใหญ่

### TaskFlow + BashOperator (hybrid)

```python
@dag(schedule="0 6 * * *", ...)
def hybrid_dag():

    @task
    def check_source():
        """Check ว่ามี data ใหม่"""
        count = get_new_row_count()
        if count == 0:
            raise AirflowSkipException("No new data")
        return count

    # BashOperator ยังใช้ได้ปกติ
    run_etl = BashOperator(task_id="run_etl", bash_command=f"{RUN} --job daily_sales")

    @task
    def notify(count: int):
        send_gchat(f"Processed {count} rows")

    row_count = check_source()
    row_count >> run_etl >> notify(row_count)
```

---

## Deferred Operators (Airflow 2.6+)

Task ที่รอนาน จะ yield worker slot กลับ → ใช้ triggerer แทน

```
Traditional Sensor:     Worker slot ถูก hold ตลอด (waste resource)
Deferrable Operator:    Yield slot → triggerer monitor → resume เมื่อ condition met
```

### ใช้งาน

```python
from airflow.sensors.filesystem import FileSensor

# Deferrable mode — ไม่เปลือง worker slot
wait = FileSensor(
    task_id="wait_for_file",
    filepath="/data/incoming/{{ ds }}.csv",
    deferrable=True,          # ← key setting
    poke_interval=60,
    timeout=7200,
)
```

### Triggerer Component

```bash
# ต้อง start triggerer process (Airflow 2.6+)
airflow triggerer

# docker-compose.airflow.yml — เพิ่ม service
# triggerer:
#   command: triggerer
#   ...
```

### เมื่อไหร่ใช้

| Scenario | ใช้ Deferrable? |
|---|---|
| Sensor รอ < 5 นาที | ❌ poke mode OK |
| Sensor รอ 5-60 นาที | ✅ reschedule mode หรือ deferrable |
| Sensor รอ > 1 ชม. | ✅ deferrable (ดีสุด) |
| Worker slots น้อย + sensor เยอะ | ✅ deferrable ทุกตัว |

---

## DAG Dependencies Visualization

### ใช้ Dataset Lineage (Airflow 2.4+)

```
[extract_users] --outlets--> [users_dataset]
[extract_orders] --outlets--> [orders_dataset]

[users_dataset, orders_dataset] --schedule--> [build_mart]
[build_mart] --outlets--> [mart_dataset]

[mart_dataset] --schedule--> [daily_report]
```

### ใช้ ExternalTaskSensor (legacy)

```
[DAG: extract_all]
    └── task: done (EmptyOperator)

[DAG: load_all]
    ├── ExternalTaskSensor(external_dag_id="extract_all", external_task_id="done")
    └── task: load

[DAG: report]
    ├── ExternalTaskSensor(external_dag_id="load_all", external_task_id="done")
    └── task: report
```

---

## Common Patterns Summary

### Pipeline Chain (Sequential DAGs)

```python
# Extract → trigger Load → trigger Report
extract_done >> TriggerDagRunOperator(trigger_dag_id="load") >> done
```

### Fan-out (1 → N)

```python
# 1 extract → trigger N downstream DAGs
extract >> [
    TriggerDagRunOperator(trigger_dag_id="load_pg"),
    TriggerDagRunOperator(trigger_dag_id="load_hdfs"),
    TriggerDagRunOperator(trigger_dag_id="notify"),
]
```

### Fan-in (N → 1)

```python
# N DAGs finish → 1 consumer runs (Dataset-aware)
with DAG("final_report", schedule=[dataset_a, dataset_b, dataset_c], ...):
    ...
```

### Conditional Execution

```python
branch >> {
    "weekday": weekday_etl >> load,
    "weekend": weekend_cleanup,
    "monthly": monthly_archive >> notify,
}
```

---

## Decision Guide

| Need | Pattern | Complexity |
|---|---|---|
| รอ file/data มา | Sensor (deferrable) | ต่ำ |
| DAG A เสร็จ → run DAG B | TriggerDagRunOperator | ต่ำ |
| dataset update → auto-trigger | Dataset scheduling | กลาง |
| 50 tables ใช้ logic เดียวกัน | Dynamic DAG (config/factory) | กลาง |
| เลือก path ตาม condition | BranchPythonOperator | ต่ำ |
| Skip ถ้าไม่มี data | ShortCircuitOperator | ต่ำ |
| Cleanup ไม่ว่า success/fail | trigger_rule="all_done" | ต่ำ |
| รอนาน ไม่เปลือง slot | Deferrable operator | กลาง |
| Pythonic DAG definition | TaskFlow API | กลาง |

---

## Anti-Patterns

| ❌ Anti-Pattern | ✅ แก้ไข |
|---|---|
| Sensor mode="poke" รอ 2 ชม. | mode="reschedule" หรือ deferrable |
| 1 giant DAG (100 tasks) | Split + TriggerDagRunOperator |
| ExternalTaskSensor chain 5 DAGs | Dataset-aware scheduling |
| Dynamic DAG จาก DB query | Static config (YAML/env) |
| Branch ไม่มี join task | เพิ่ม join with trigger_rule="none_failed_min_one_success" |
| TaskFlow ส่ง DataFrame ผ่าน XCom | ส่ง file path แทน |
