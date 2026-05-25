# DAG File Best Practices

แนวทางเขียน DAG file ให้ parse เร็ว, maintain ง่าย, scale ได้

---

## File Structure

### One DAG per File (recommended)

```
dags/
├── daily_sales_etl.py          # 1 DAG
├── hourly_incremental.py       # 1 DAG
├── weekly_archive.py           # 1 DAG
└── utils/                      # shared helpers (ไม่ใช่ DAG)
    └── __init__.py
```

**ทำไม:**
- Parse เร็วกว่า (Airflow parse file 1 ตัว = 1 process)
- Debug ง่าย (ชื่อไฟล์ = ชื่อ DAG)
- git blame ชัดเจน

### ❌ หลาย DAGs ในไฟล์เดียว

```python
# ❌ BAD — ไฟล์ใหญ่, parse ช้า, git conflict
with DAG("dag_a", ...) as dag_a:
    ...

with DAG("dag_b", ...) as dag_b:
    ...
```

---

## Import Strategy

### Lazy Import (สำคัญมาก)

Airflow re-parse ทุก DAG file ทุก 30-120 วินาที — **top-level import ทำทุกรอบ**

```python
# ❌ BAD — import pandas ทุก parse cycle (200ms+ per parse)
import pandas as pd
import numpy as np
from connector.database.postgres import PostgresConnector

with DAG(...) as dag:
    task = PythonOperator(task_id="t", python_callable=my_fn)

# ✅ GOOD — import ตอน execute เท่านั้น
def my_fn(**context):
    import pandas as pd
    from connector.database.postgres import PostgresConnector
    # ... logic here

with DAG(...) as dag:
    task = PythonOperator(task_id="t", python_callable=my_fn)
```

### Benchmark

| Import Pattern | Parse Time | 100 DAG files |
|---|---|---|
| `import pandas` at top | 180ms/file | 18s per parse cycle |
| No heavy import | 5ms/file | 0.5s per parse cycle |
| **ต่าง 36x** | | |

### Top-level Imports ที่ OK

```python
# OK — Airflow modules (already loaded)
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# OK — lightweight custom modules (ไม่ import heavy libs ภายใน)
from utils.notify import on_failure_callback
```

### Top-level Imports ที่ห้าม

```python
# ❌ ห้าม — heavy libraries
import pandas as pd
import numpy as np
import pyarrow
import polars as pl
import sqlalchemy

# ❌ ห้าม — connector ที่ establish connection ตอน import
from connector.database.postgres import PostgresConnector

# ❌ ห้าม — any module ที่ import heavy lib ภายใน
from utils.upsert import upsert_postgres  # imports psycopg2 at module level
```

---

## Static DAG Definition

### ห้ามรัน dynamic logic ตอน parse

```python
# ❌ BAD — query DB ทุก parse cycle (ทุก 30s!)
import psycopg2
conn = psycopg2.connect(...)
tables = [row[0] for row in conn.execute("SELECT table_name FROM ...")]

with DAG(...) as dag:
    for table in tables:
        PythonOperator(task_id=f"sync_{table}", ...)

# ❌ BAD — read file ทุก parse cycle
import json
with open("/config/tables.json") as f:
    tables = json.load(f)

# ❌ BAD — API call ตอน parse
import requests
config = requests.get("http://config-service/tables").json()
```

### ✅ Static List

```python
# ✅ GOOD — hardcode list
TABLES = ["users", "orders", "products", "payments", "inventory"]

with DAG(...) as dag:
    for table in TABLES:
        PythonOperator(
            task_id=f"sync_{table}",
            python_callable=sync_table,
            op_kwargs={"table": table},
        )
```

### ✅ Config File (read once, cache)

```python
# ✅ OK — Airflow Variable (cached in DB, ไม่ query ทุก parse)
from airflow.models import Variable

# Variable ถูก cache — ไม่ query DB ทุก parse cycle
TABLES = Variable.get("sync_tables", deserialize_json=True, default_var=["users"])
```

> **Warning:** `Variable.get()` ยังมี overhead เล็กน้อย — ถ้า DAG เยอะมาก ใช้ environment variable แทน

### ✅ Environment Variable (fastest)

```python
import os

# ✅ BEST for dynamic config — ไม่ query DB, ไม่ read file
TABLES = os.environ.get("ETL_SYNC_TABLES", "users,orders,products").split(",")
```

---

## Task Callable Pattern

### แยก Logic ออกจาก DAG File

```python
# ❌ BAD — logic อยู่ใน DAG file (ยาว, test ยาก)
def extract_users(**context):
    import pandas as pd
    from connector.database.postgres import PostgresConnector
    with PostgresConnector(...) as pg:
        df = pd.read_sql("SELECT * FROM users", pg.conn)
        # 50 lines of transform...
        # 30 lines of validation...
        upsert_postgres(...)

# ✅ GOOD — DAG file แค่ wire tasks, logic อยู่ใน jobs/
def extract_users(**context):
    from jobs.sync_users import SyncUsersJob
    from utils.config import Settings
    job = SyncUsersJob()
    job.run(Settings())
```

### Template Pattern (BashOperator)

```python
# ✅ BEST for this template — ใช้ main.py entry point
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from utils.notify import on_failure_callback, on_retry_callback

PROJECT_DIR = os.environ.get("ETL_PROJECT_DIR", "/opt/airflow/dags/etl-template")
RUN = f"cd {PROJECT_DIR} && uv run main.py"

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": on_failure_callback,
    "on_retry_callback": on_retry_callback,
}

with DAG("daily_sales", default_args=default_args, schedule="0 6 * * *",
         start_date=datetime(2025, 1, 1), catchup=False, tags=["etl", "sales"]):

    BashOperator(task_id="sync_sales", bash_command=f"{RUN} --job sync_sales")
```

---

## DAG Parameters

### ต้องมีเสมอ

```python
with DAG(
    dag_id="my_dag",                        # unique, descriptive
    schedule="0 6 * * *",                   # cron expression
    start_date=datetime(2025, 1, 1),        # fixed date (ห้าม datetime.now())
    catchup=False,                          # ห้าม True ใน production (backfill ด้วย CLI)
    tags=["etl", "sales", "postgres"],      # สำหรับ filter ใน UI
    max_active_runs=1,                      # ป้องกัน overlap (ETL ส่วนใหญ่)
    default_args=default_args,
) as dag:
    ...
```

### Common Mistakes

```python
# ❌ start_date = datetime.now() — DAG จะไม่ schedule!
start_date=datetime.now()

# ❌ catchup=True โดยไม่ตั้งใจ — Airflow จะ backfill ทุก interval ตั้งแต่ start_date
catchup=True

# ❌ schedule ถี่เกินจำเป็น
schedule="* * * * *"  # ทุกนาที — scheduler overload
```

---

## Naming Convention

### DAG ID

```python
# Pattern: {frequency}_{domain}_{action}
"daily_sales_etl"
"hourly_orders_incremental"
"weekly_archive_hdfs"
"monthly_report_summary"
```

### Task ID

```python
# Pattern: {verb}_{object}
"extract_users"
"transform_orders"
"load_dim_customer"
"validate_fact_sales"
"notify_success"
```

### File Name = DAG ID

```
dags/daily_sales_etl.py         → dag_id="daily_sales_etl"
dags/hourly_orders_incremental.py → dag_id="hourly_orders_incremental"
```

---

## Tags & Documentation

```python
with DAG(
    dag_id="daily_sales_etl",
    tags=["etl", "sales", "postgres", "daily"],    # ค้นหาใน UI
    doc_md="""
    ## Daily Sales ETL
    - **Source:** MSSQL `erp.dbo.sales`
    - **Target:** PostgreSQL `dwh.fact_sales`
    - **Schedule:** ทุกวัน 06:00
    - **Owner:** data-team
    - **SLA:** ต้องเสร็จก่อน 07:00
    """,
    ...
) as dag:
    ...
```

---

## Task Dependencies

### ใช้ Bitshift Operators

```python
# ✅ อ่านง่าย
extract >> transform >> load >> validate

# ✅ Parallel → converge
[extract_a, extract_b, extract_c] >> transform >> load

# ✅ Fan-out
extract >> [load_pg, load_mssql, load_hdfs]
```

### ❌ หลีกเลี่ยง

```python
# ❌ set_downstream/set_upstream — อ่านยาก
extract.set_downstream(transform)
transform.set_downstream(load)

# ❌ ซับซ้อนเกินไป — แยก DAG
a >> b >> c >> d >> e >> f >> g >> h  # ยาวเกินไป
```

### Task Group (จัด task ที่เกี่ยวข้อง)

```python
from airflow.utils.task_group import TaskGroup

with DAG(...) as dag:
    with TaskGroup("extract") as extract_group:
        extract_users = PythonOperator(task_id="users", ...)
        extract_orders = PythonOperator(task_id="orders", ...)
        extract_products = PythonOperator(task_id="products", ...)

    with TaskGroup("load") as load_group:
        load_dim = PythonOperator(task_id="dimensions", ...)
        load_fact = PythonOperator(task_id="facts", ...)

    extract_group >> load_group
```

---

## Timeout & SLA

```python
from airflow.operators.python import PythonOperator
from datetime import timedelta

# Task timeout — kill ถ้ารันนานเกิน
task = PythonOperator(
    task_id="extract",
    python_callable=extract_fn,
    execution_timeout=timedelta(hours=1),   # ← kill ถ้า > 1 ชม.
)

# DAG-level timeout
with DAG(
    dag_id="my_dag",
    dagrun_timeout=timedelta(hours=3),      # ← kill ทั้ง DAG run ถ้า > 3 ชม.
    ...
) as dag:
    ...
```

### SLA Miss

```python
# แจ้งถ้า task ไม่เสร็จภายใน SLA
task = PythonOperator(
    task_id="critical_load",
    python_callable=load_fn,
    sla=timedelta(hours=1),                 # ← แจ้ง SLA miss ถ้า > 1 ชม.
)

# SLA miss callback
def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    from utils.notify import send_gchat
    tasks = ", ".join(t.task_id for t in task_list)
    send_gchat(f"⚠️ SLA Miss: {dag.dag_id} — tasks: {tasks}")

with DAG(
    dag_id="my_dag",
    sla_miss_callback=sla_miss_callback,
    ...
) as dag:
    ...
```

---

## Idempotency in DAG

```python
# ✅ ใช้ execution_date (logical date) — ไม่ใช่ datetime.now()
def extract(**context):
    exec_date = context["execution_date"]
    start = exec_date.strftime("%Y-%m-%d")
    end = (exec_date + timedelta(days=1)).strftime("%Y-%m-%d")

    import pandas as pd
    df = pd.read_sql(f"""
        SELECT * FROM source WHERE dt >= '{start}' AND dt < '{end}'
    """, conn)
    return df

# ✅ ทำให้ safe to re-run (UPSERT / DELETE+INSERT)
def load(**context):
    from utils.upsert import upsert_postgres
    df = context["task_instance"].xcom_pull(task_ids="extract")
    upsert_postgres(conn, df, "target", conflict_columns=["id"])
```

---

## XCom — Passing Data Between Tasks

### เมื่อไหร่ใช้ XCom

| Data Size | Method | ตัวอย่าง |
|---|---|---|
| < 48 KB | XCom (DB) | status, count, file path |
| 48 KB – 10 MB | XCom + serialization | small DataFrame (JSON) |
| > 10 MB | External storage | MinIO/HDFS/temp file |

### ✅ ใช้ XCom สำหรับ metadata

```python
def extract(**context):
    # ... extract logic ...
    context["task_instance"].xcom_push(key="row_count", value=len(df))
    context["task_instance"].xcom_push(key="file_path", value="/tmp/data.parquet")
    return {"status": "ok", "rows": len(df)}

def load(**context):
    ti = context["task_instance"]
    file_path = ti.xcom_pull(task_ids="extract", key="file_path")
    row_count = ti.xcom_pull(task_ids="extract", key="row_count")
    # ... load logic ...
```

### ❌ ห้ามส่ง DataFrame ผ่าน XCom

```python
# ❌ BAD — serialize ทั้ง DataFrame ลง metadata DB
def extract(**context):
    df = pd.read_sql(...)
    return df  # pickle → metadata DB → OOM

# ✅ GOOD — ส่ง path, let next task read
def extract(**context):
    df = pd.read_sql(...)
    path = "/tmp/extract_2025-01-15.parquet"
    df.to_parquet(path)
    return {"path": path, "rows": len(df)}
```

---

## Testing DAGs

```bash
# 1. Parse test — ตรวจ syntax + import
python dags/my_dag.py

# 2. DAG validation
airflow dags test my_dag 2025-01-15

# 3. Task test (dry run)
airflow tasks test my_dag extract 2025-01-15

# 4. Python test (pytest)
```

```python
# tests/test_dags.py
import importlib
import glob

def test_dag_imports():
    """ทุก DAG file ต้อง parse ได้ไม่ error"""
    dag_files = glob.glob("dags/*.py")
    for f in dag_files:
        module = f.replace("/", ".").replace(".py", "")
        importlib.import_module(module)  # ต้องไม่ raise

def test_dag_has_tags():
    """ทุก DAG ต้องมี tags"""
    from airflow.models import DagBag
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"DAG {dag_id} has no tags"
```

---

## Anti-Patterns

| ❌ Anti-Pattern | ✅ แก้ไข |
|---|---|
| Heavy import at top-level | Lazy import inside callable |
| DB query at parse time | Static list / env var / Variable |
| `datetime.now()` in data filter | `context["execution_date"]` |
| `catchup=True` ไม่ตั้งใจ | `catchup=False` + manual backfill |
| Giant single DAG (50+ tasks) | Split เป็น multiple DAGs + trigger |
| No timeout | `execution_timeout` + `dagrun_timeout` |
| DataFrame in XCom | Temp file path in XCom |
| Logic in DAG file | Logic in `jobs/` module |
| No retry | `retries=2-3` + exponential backoff |
| No pool on DB tasks | `pool="postgres_pool"` |

---

## DAG Template Checklist

```python
# ✅ Complete DAG template
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from utils.notify import on_failure_callback, on_retry_callback, on_success_callback

PROJECT_DIR = os.environ.get("ETL_PROJECT_DIR", "/opt/airflow/dags/etl-template")
RUN = f"cd {PROJECT_DIR} && uv run main.py"

default_args = {
    "owner": "data-team",                           # ✅ owner
    "retries": 2,                                   # ✅ retry
    "retry_delay": timedelta(minutes=5),            # ✅ backoff
    "retry_exponential_backoff": True,              # ✅ exponential
    "execution_timeout": timedelta(hours=2),        # ✅ timeout
    "on_failure_callback": on_failure_callback,     # ✅ alert
    "on_retry_callback": on_retry_callback,         # ✅ alert
}

with DAG(
    dag_id="daily_sales_etl",                       # ✅ descriptive name
    default_args=default_args,
    schedule="0 6 * * *",                           # ✅ explicit schedule
    start_date=datetime(2025, 1, 1),                # ✅ fixed date
    catchup=False,                                  # ✅ no accidental backfill
    max_active_runs=1,                              # ✅ no overlap
    dagrun_timeout=timedelta(hours=4),              # ✅ DAG timeout
    tags=["etl", "sales", "daily", "postgres"],     # ✅ tags
    on_success_callback=on_success_callback,        # ✅ success alert
    doc_md="Daily sales ETL: ERP(MSSQL) → DWH(PG)",# ✅ documentation
) as dag:

    extract = BashOperator(
        task_id="extract_sales",
        bash_command=f"{RUN} --job extract_sales",
        pool="source_db_pool",                      # ✅ pool
    )

    load = BashOperator(
        task_id="load_sales",
        bash_command=f"{RUN} --job load_sales",
        pool="postgres_pool",                       # ✅ pool
    )

    extract >> load                                 # ✅ clear dependency
```

---

## Summary

| หมวด | กฎ |
|---|---|
| **File** | 1 DAG / file, filename = dag_id |
| **Import** | Lazy import heavy libs inside callable |
| **Parse** | Static definition, no DB/API/file IO at parse time |
| **Logic** | อยู่ใน `jobs/` module, DAG file แค่ wire tasks |
| **Schedule** | `catchup=False`, fixed `start_date`, `max_active_runs=1` |
| **Timeout** | ทุก task มี `execution_timeout`, DAG มี `dagrun_timeout` |
| **Alert** | `on_failure_callback`, `on_retry_callback` ทุก DAG |
| **Pool** | DB tasks ใส่ pool เสมอ |
| **XCom** | metadata only (path, count), ห้ามส่ง DataFrame |
| **Test** | parse test + `airflow dags test` + pytest |
