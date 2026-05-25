# Airflow Pools & Workers

จัดการ resource allocation ด้วย Pool และ Celery Worker — ควบคุม concurrency, แบ่ง workload, ป้องกัน resource contention

---

## Pools — ทำไมต้องใช้

Pool = **global concurrency limiter** — จำกัดจำนวน task ที่รันพร้อมกันในหมวดเดียวกัน

### ปัญหาที่ Pool แก้ได้

| ปัญหา | ไม่มี Pool | มี Pool |
|---|---|---|
| DB connection exhausted | 20 tasks query PG พร้อมกัน → `max_connections` เต็ม | จำกัด 5 tasks ต่อ PG |
| API rate limit | 50 tasks เรียก API → ถูก block | จำกัด 3 tasks ต่อ API |
| OOM on worker | 8 heavy tasks รัน pandas พร้อมกัน → 10+ GB | จำกัด 2 heavy tasks |
| Source DB slow | ETL ทุก DAG query source DB → production slow | จำกัด source query |

---

## Pool Configuration

### สร้าง Pool (CLI)

```bash
# สร้าง pool
airflow pools set postgres_pool 5 "PostgreSQL connection limit"
airflow pools set mssql_pool 3 "MSSQL connection limit"
airflow pools set heavy_etl 2 "Heavy ETL (high RAM)"
airflow pools set api_pool 3 "External API rate limit"
airflow pools set hdfs_pool 4 "HDFS/Impala connections"

# ดู pool ทั้งหมด
airflow pools list

# ลบ pool
airflow pools delete old_pool
```

### สร้าง Pool (Python/DAG)

```python
from airflow.models import Pool
from airflow.utils.session import create_session

# ใส่ใน DAG file หรือ initialization script
with create_session() as session:
    pools = [
        Pool(pool="postgres_pool", slots=5, description="PG connections"),
        Pool(pool="mssql_pool", slots=3, description="MSSQL connections"),
        Pool(pool="heavy_etl", slots=2, description="Heavy RAM tasks"),
        Pool(pool="api_pool", slots=3, description="External API"),
    ]
    for pool in pools:
        existing = session.query(Pool).filter(Pool.pool == pool.pool).first()
        if not existing:
            session.add(pool)
    session.commit()
```

### ใช้ Pool ใน Task

```python
from airflow.operators.python import PythonOperator

# กำหนด pool ที่ task level
extract_pg = PythonOperator(
    task_id="extract_from_postgres",
    python_callable=extract_fn,
    pool="postgres_pool",        # ← ใช้ pool
    pool_slots=1,                # ← ใช้กี่ slot (default=1)
)

# Heavy task ใช้ 2 slots (เท่ากับ limit 1 heavy task ถ้า pool=2)
heavy_transform = PythonOperator(
    task_id="heavy_pandas_transform",
    python_callable=heavy_fn,
    pool="heavy_etl",
    pool_slots=2,                # ← ใช้ 2 slots = กัน resource มากขึ้น
)
```

### Priority Weight

เมื่อ pool เต็ม — task ไหนได้รันก่อน?

```python
# priority_weight สูง = ได้รันก่อน (default=1)
critical_task = PythonOperator(
    task_id="critical_report",
    python_callable=report_fn,
    pool="postgres_pool",
    priority_weight=10,          # ← จอง slot ก่อน task อื่น
)

low_priority = PythonOperator(
    task_id="archive_old_data",
    python_callable=archive_fn,
    pool="postgres_pool",
    priority_weight=1,           # ← รอถ้า pool เต็ม
)
```

### Priority Rule

```python
# weight_rule กำหนดวิธีคำนวณ effective weight
from airflow.utils.weight_rule import WeightRule

task = PythonOperator(
    task_id="my_task",
    pool="postgres_pool",
    priority_weight=5,
    weight_rule=WeightRule.DOWNSTREAM,  # weight = sum ของ downstream tasks
    # WeightRule.UPSTREAM   → weight = sum ของ upstream tasks
    # WeightRule.ABSOLUTE   → weight = ค่าที่กำหนด (5)
)
```

---

## Pool Design — Production Setup

### Recommended Pools

```
┌─────────────────────────────────────────────────────────┐
│                    Pool Layout                           │
├─────────────────────────────────────────────────────────┤
│  default_pool (128 slots)     ← Airflow built-in        │
│  postgres_pool (5 slots)      ← PG max_connections ÷ 2  │
│  mssql_pool (3 slots)         ← MSSQL license/perf      │
│  impala_pool (4 slots)        ← Impala query concurrency │
│  heavy_etl (2 slots)          ← High RAM tasks          │
│  api_external (3 slots)       ← API rate limit          │
│  source_db_pool (4 slots)     ← Production source DB    │
└─────────────────────────────────────────────────────────┘
```

### Pool Sizing กฎ

| Pool | Slots | คำนวณจาก |
|---|---|---|
| `postgres_pool` | 5 | `max_connections` (100) ÷ 20 (เผื่อ app อื่น) |
| `mssql_pool` | 3 | MSSQL CPU cores ÷ 2 (ป้องกัน lock) |
| `impala_pool` | 4 | Impala `--fe_service_threads` ÷ 4 |
| `heavy_etl` | 2 | Worker RAM (10GB) ÷ max task RAM (4GB) |
| `api_external` | 3 | API rate limit ÷ avg task duration |
| `source_db_pool` | 4 | Source DB max load ที่ DBA อนุญาต |

### ตัวอย่าง DAG ที่ใช้ Pool

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "daily_etl_with_pools",
    default_args=default_args,
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:

    # Source query — จำกัดไม่ให้ query source DB เกิน 4 พร้อมกัน
    extract_users = PythonOperator(
        task_id="extract_users",
        python_callable=extract_users_fn,
        pool="source_db_pool",
    )

    extract_orders = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders_fn,
        pool="source_db_pool",
    )

    # Heavy transform — จำกัด 2 พร้อมกัน (RAM)
    transform = PythonOperator(
        task_id="transform_heavy",
        python_callable=transform_fn,
        pool="heavy_etl",
        pool_slots=2,
    )

    # Load to target PG — จำกัด connection
    load = PythonOperator(
        task_id="load_postgres",
        python_callable=load_fn,
        pool="postgres_pool",
    )

    [extract_users, extract_orders] >> transform >> load
```

---

## Celery Workers — Architecture

### Single Worker (default ใน template)

```
Scheduler → Redis → Worker (concurrency=8)
                       ├── Task slot 1
                       ├── Task slot 2
                       ├── ...
                       └── Task slot 8
```

### Multiple Workers (scale out)

```
                    ┌── Worker-default (C=8, queues=default)
Scheduler → Redis ──┼── Worker-etl (C=4, queues=etl)
                    └── Worker-heavy (C=2, queues=heavy)
```

---

## Worker Configuration

### Concurrency Tuning

```ini
# airflow.cfg
[celery]
# Global: ทุก task ของ worker นี้
worker_concurrency = 8
```

```bash
# Override per worker
airflow celery worker --concurrency 4 --queues heavy
```

| Workload Type | Concurrency | RAM/Task | ทำไม |
|---|---|---|---|
| I/O bound (DB query → DB insert) | 8-16 | ~0.5 GB | Mostly waiting for network |
| CPU bound (pandas transform) | 4-6 | ~1.5 GB | CPU limited |
| Heavy (large DataFrame, PyArrow) | 2-3 | ~3-4 GB | RAM limited |
| Mixed (production default) | 8 | ~1.2 GB | Balance |

### Queue Routing

แยก queue ตาม workload type:

```python
# DAG — กำหนด queue per task
light_task = PythonOperator(
    task_id="light_query",
    python_callable=light_fn,
    queue="default",       # ← worker-default จะ pick
)

heavy_task = PythonOperator(
    task_id="heavy_transform",
    python_callable=heavy_fn,
    queue="heavy",         # ← worker-heavy จะ pick
)

docker_task = DockerOperator(
    task_id="isolated_job",
    image="harbor.company.com/etl/my-job:latest",
    queue="docker",        # ← worker-docker จะ pick
)
```

### Start Workers ตาม Queue

```bash
# Worker 1: default queue (concurrency=8)
airflow celery worker \
    --concurrency 8 \
    --queues default \
    --hostname worker-default@%h

# Worker 2: ETL queue (concurrency=4, เผื่อ RAM)
airflow celery worker \
    --concurrency 4 \
    --queues etl \
    --hostname worker-etl@%h

# Worker 3: Heavy queue (concurrency=2, high RAM)
airflow celery worker \
    --concurrency 2 \
    --queues heavy \
    --hostname worker-heavy@%h
```

### Systemd — Multiple Workers

```ini
# /etc/systemd/system/airflow-worker-default.service
[Service]
ExecStart=/usr/local/bin/airflow celery worker --concurrency 8 --queues default --hostname worker-default@%h
MemoryMax=6G

# /etc/systemd/system/airflow-worker-heavy.service
[Service]
ExecStart=/usr/local/bin/airflow celery worker --concurrency 2 --queues heavy --hostname worker-heavy@%h
MemoryMax=8G
```

---

## Worker Prefetch & Ack

```ini
[celery]
# Prefetch = กี่ task ดึงมารอใน buffer ก่อน execute
# 1 = ดึงทีละ 1 (fair distribution, ช้านิดหน่อย)
# 4 = ดึง 4 มารอ (เร็วกว่า, แต่ task อาจ stuck ถ้า worker crash)
worker_prefetch_multiplier = 1       # production: ใช้ 1

# Ack late = ack หลัง execute สำเร็จ
# True = ถ้า worker crash → task re-queue อัตโนมัติ (at-least-once)
# False = ack ตอนรับ → ถ้า crash → task หาย
task_acks_late = True                # production: ใช้ True
```

### At-least-once vs At-most-once

| Setting | Behavior | Risk |
|---|---|---|
| `acks_late=True` + `prefetch=1` | At-least-once (retry on crash) | อาจ duplicate → ต้อง idempotent |
| `acks_late=False` + `prefetch=4` | At-most-once (fast, no retry) | อาจ lost task on crash |

**Production recommendation:** `acks_late=True` + `prefetch=1` + **idempotent tasks** (UPSERT)

---

## Worker Autoscaling

### Celery Autoscale (built-in)

```bash
# Min 2, Max 8 tasks concurrent — scale ตาม queue depth
airflow celery worker --autoscale 8,2 --queues default
```

| ถ้า queue depth | Concurrency |
|---|---|
| 0 tasks | 2 (min) |
| 1-4 tasks | 4 (scaling up) |
| 5+ tasks | 8 (max) |

**ข้อดี:** ลด RAM usage ตอน idle
**ข้อเสีย:** Scale ช้า (Celery fork/kill workers)

### Horizontal Autoscale (KEDA / custom)

```python
# check_queue_depth.py — trigger เพิ่ม/ลด worker container
import redis

r = redis.Redis(host="localhost", port=6379, db=0)
queue_depth = r.llen("default")  # จำนวน task ใน queue

if queue_depth > 20:
    # เพิ่ม worker (docker/systemd/k8s)
    scale_up_worker()
elif queue_depth == 0:
    # ลด worker (เหลือ 1 min)
    scale_down_worker()
```

---

## Worker Health & Monitoring

### Flower (Celery Monitor)

```bash
# Start Flower (included in docker-compose.airflow.yml)
airflow celery flower --port 5555

# http://localhost:5555
# - ดู worker status, active tasks, queue depth
# - ดู task execution time, success/failure rate
```

### Health Check

```bash
# Check worker alive
airflow celery worker --pid /tmp/airflow-worker.pid
celery -A airflow.executors.celery_executor.app inspect ping

# Check specific worker
celery -A airflow.executors.celery_executor.app inspect active \
    --destination worker-default@myhost
```

### Worker Restart Strategy

```ini
# systemd — restart on crash
[Service]
Restart=always
RestartSec=10

# Celery — restart worker ทุก 100 tasks (ป้องกัน memory leak)
# airflow.cfg
[celery]
worker_max_tasks_per_child = 100
```

```bash
# Graceful restart (finish current tasks แล้ว restart)
airflow celery worker --pid /tmp/worker.pid
kill -TERM $(cat /tmp/worker.pid)

# Warm shutdown — Celery revoke ค้าง tasks แล้ว restart
celery -A airflow.executors.celery_executor.app control shutdown
```

---

## Pool + Worker — Combined Strategy

### Production Layout (8C/16GB, 50+ DAGs)

```
┌─────────────────────────────────────────────┐
│ Pools (logical limits):                      │
│   postgres_pool: 5 slots                     │
│   mssql_pool: 3 slots                       │
│   heavy_etl: 2 slots                        │
│   source_db_pool: 4 slots                   │
├─────────────────────────────────────────────┤
│ Workers (physical resources):                │
│   worker-default (C=6, queue=default)        │
│   worker-heavy (C=2, queue=heavy)            │
├─────────────────────────────────────────────┤
│ Task routing:                                │
│   Light ETL → queue=default, pool=source_db  │
│   Heavy ETL → queue=heavy, pool=heavy_etl    │
│   PG load   → queue=default, pool=pg_pool    │
└─────────────────────────────────────────────┘
```

### ความต่าง Pool vs Queue

| | Pool | Queue |
|---|---|---|
| **Scope** | Global (ข้าม worker) | Per worker |
| **Limit** | Concurrency limit | Worker assignment |
| **Use case** | จำกัด resource (DB conn, API) | แยก workload type |
| **Config** | task level (`pool=`) | task level (`queue=`) |
| **Scale** | เพิ่ม slots | เพิ่ม workers |

**กฎง่ายๆ:**
- **Pool** = "ไม่เกิน X tasks query DB นี้พร้อมกัน" (logical limit)
- **Queue** = "task นี้ให้ worker ตัวนี้รัน" (physical routing)

---

## Monitoring Queries

```sql
-- ดู pool usage (Airflow metadata DB)
SELECT pool, slots, COUNT(*) as running
FROM task_instance
WHERE state = 'running'
GROUP BY pool, slots;

-- ดู queue depth
SELECT queue, state, COUNT(*) 
FROM task_instance
WHERE state IN ('queued', 'scheduled', 'running')
GROUP BY queue, state;

-- ดู task ที่ waiting for pool
SELECT dag_id, task_id, pool, state, queued_dttm
FROM task_instance
WHERE state = 'scheduled' AND pool != 'default_pool'
ORDER BY queued_dttm;
```

---

## Summary

| Config | ค่า Production | หน้าที่ |
|---|---|---|
| `worker_concurrency` | 8 (default), 2-4 (heavy) | Physical task slots |
| `parallelism` | 24 | Global max tasks |
| `max_active_tasks_per_dag` | 8 | Per-DAG limit |
| Pool slots | 2-5 per pool | Resource protection |
| Queue routing | default, etl, heavy | Workload isolation |
| `worker_prefetch_multiplier` | 1 | Fair scheduling |
| `task_acks_late` | True | Crash recovery |
| `worker_max_tasks_per_child` | 100 | Memory leak prevention |
