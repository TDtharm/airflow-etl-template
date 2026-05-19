# Airflow Configuration

วิธี deploy ETL template บน Airflow (on-premise) — config เชิงลึกสำหรับ scheduler, worker, และ performance tuning

---

## Directory Structure บน Airflow Server

```
/opt/airflow/
├── dags/
│   └── etl-template/           ← Jenkins git pull มาไว้ที่นี่
│       ├── dags/
│       │   └── example_dag.py  ← Airflow scanner เจอไฟล์นี้
│       ├── main.py
│       ├── utils/
│       ├── connector/
│       ├── job/
│       ├── .env                ← secrets อยู่บน server เท่านั้น
│       └── pyproject.toml
├── logs/
└── airflow.cfg
```

---

## Production Config — 8 Core / 16 GB (ขั้นต่ำ)

```
Spec: 8 vCPU, 16 GB RAM, SSD
OS: RHEL 8 / Rocky 9 / Ubuntu 22.04
Components: Scheduler + Worker + Webserver + Redis (same node)
Metadata DB: PostgreSQL (แยก node หรือ same node)
```

**Resource allocation:**
```
Scheduler     : 2 cores, 2 GB RAM (parsing + scheduling)
Celery Worker : 4 cores, 10 GB RAM (concurrency=8, ~1.2 GB/task)
Webserver     : 1 core, 2 GB RAM (4 gunicorn workers)
Redis         : 1 core, 1 GB RAM (broker)
OS overhead   : ~1 GB
```

---

## airflow.cfg — Core

```ini
[core]
# CeleryExecutor + Redis — production default
executor = CeleryExecutor

# DAG folder
dags_folder = /opt/airflow/dags

# ให้ Airflow import module จาก project ได้
dagbag_import_timeout = 120

# Max จำนวน task ที่ system ทั้งหมดรันพร้อมกัน
# 8-core: ไม่ควรเกิน 24 (เผื่อ scheduler + webserver)
parallelism = 24

# Max จำนวน task instance ที่รัน active ใน 1 DAG run
max_active_tasks_per_dag = 8

# Max จำนวน DAG run active พร้อมกัน per DAG
max_active_runs_per_dag = 2

# Default timezone
default_timezone = Asia/Bangkok
```

---

## airflow.cfg — Scheduler

```ini
[scheduler]
# ------- DAG Discovery -------

# scan หา DAG files ใหม่ทุก 60 วินาที
dag_dir_list_interval = 60

# re-parse แต่ละ DAG file ทุก 30 วินาที
min_file_process_interval = 30

# จำนวน process parse DAG (8-core: ใช้ 2 cores สำหรับ scheduler)
parsing_processes = 2

# ------- Scheduling -------

# scheduler loop heartbeat
scheduler_heartbeat_sec = 5

# task ไม่ส่ง heartbeat ใน 5 นาที → zombie
scheduler_zombie_task_threshold = 300

# dispatch task ต่อ loop
max_tis_per_query = 512

# ------- Performance -------
use_row_level_locking = True
orphaned_tasks_check_interval = 300
catchup_by_default = False
```

---

## airflow.cfg — Celery + Redis

```ini
[celery]
# Redis broker (same node — port 6379)
broker_url = redis://localhost:6379/0

# Result backend → metadata DB
result_backend = db+postgresql://airflow:password@localhost:5432/airflow

# 8 concurrent tasks per worker (8-core, 10 GB for worker)
# ≈ 1.2 GB RAM per task (Pandas/PyArrow ETL)
worker_concurrency = 8

# ดึง task มารอใน buffer 1 ตัว (ป้องกัน task stuck ใน prefetch)
worker_prefetch_multiplier = 1

# Ack หลัง execute — ถ้า worker crash ระหว่าง task จะ re-queue
task_acks_late = True

# Task timeout (kill ถ้ารันเกิน 4 ชม.)
task_time_to_live = 14400

# Queue routing
default_queue = default
```

---

## airflow.cfg — Redis

```ini
# /etc/redis/redis.conf (production)
bind 127.0.0.1
port 6379
maxmemory 1gb
maxmemory-policy allkeys-lru
appendonly yes
tcp-keepalive 300
```

---

## Architecture (Single Node — 8 Core / 16 GB)

```
┌──────────────────────────────────────────────────┐
│                 Server (8C/16GB)                  │
├──────────────────────────────────────────────────┤
│  Scheduler (2C/2GB)                              │
│    ├── DAG parser (2 processes)                  │
│    └── Task dispatcher → Redis                   │
│                                                  │
│  Redis Broker (1C/1GB)                           │
│    └── Task queue                                │
│                                                  │
│  Celery Worker (4C/10GB)                         │
│    ├── concurrency=8 (I/O bound ETL)             │
│    └── queues: default, etl                      │
│                                                  │
│  Webserver (1C/2GB)                              │
│    └── gunicorn × 4 workers                      │
│                                                  │
│  PostgreSQL (metadata) — แยก node ได้ถ้าโหลดเยอะ │
└──────────────────────────────────────────────────┘
```

**Start worker:**
```bash
airflow celery worker --concurrency 8 --queues default,etl
```

**Scale out (เพิ่ม node):**
```
                         ┌── Worker Node 2 (8C/16GB, concurrency=8)
Scheduler → Redis Broker ─┼── Worker Node 3 (8C/16GB, concurrency=8)
                         └── Worker Node 1 (this server, concurrency=8)
```

**Worker sizing (8-core 16GB):**

| Workload | `worker_concurrency` | RAM ที่ใช้ | หมายเหตุ |
|---|---|---|---|
| Light ETL (DB query → DB insert) | 8 | ~4 GB | I/O bound |
| Medium ETL (Pandas transform) | 6 | ~7 GB | ~1.2 GB/task |
| Heavy ETL (large DataFrame/PyArrow) | 4 | ~8 GB | ~2 GB/task |
| Mixed (default) | **8** | ~10 GB | prod default |

---

## airflow.cfg — Logging

```ini
[logging]
# Log level (DEBUG/INFO/WARNING/ERROR)
logging_level = INFO

# Task log retention
# ลบ log เก่ากว่า X วัน (ป้องกัน disk full)
# ต้อง setup log rotation เอง (logrotate หรือ airflow CLI)

# Remote logging (optional — ส่ง log ไป S3/GCS/MinIO)
# remote_logging = True
# remote_log_conn_id = minio_default
# remote_base_log_folder = s3://airflow-logs/
```

---

## airflow.cfg — Database (Metadata)

```ini
[database]
# Production: ใช้ PostgreSQL (ห้ามใช้ SQLite)
sql_alchemy_conn = postgresql+psycopg2://airflow:password@localhost:5432/airflow

# Connection pool (8-core server)
sql_alchemy_pool_size = 10
sql_alchemy_max_overflow = 20
sql_alchemy_pool_recycle = 1800
sql_alchemy_pool_pre_ping = True
```

**PostgreSQL tuning (สำหรับ metadata DB บน same server):**
```ini
# /etc/postgresql/15/main/postgresql.conf
max_connections = 100
shared_buffers = 1GB
effective_cache_size = 2GB
work_mem = 16MB
max_wal_size = 1GB
```

---

## airflow.cfg — Kerberos

```ini
[kerberos]
# Airflow จะ kinit ให้อัตโนมัติ ทุก reinit_frequency วินาที
keytab = /etc/security/keytabs/airflow.keytab
principal = airflow@REALM.COM
reinit_frequency = 3600
ccache = /tmp/airflow_krb5_ccache
```

---

## .airflowignore

สร้างไฟล์ `.airflowignore` ใน `/opt/airflow/dags/etl-template/` เพื่อบอก Airflow ไม่ต้อง scan ไฟล์ที่ไม่ใช่ DAG:

```
connector/
job/
utils/
tests/
docs/
model/
.venv/
```

> Airflow จะ scan เฉพาะ `dags/` folder — ลดเวลา parsing

---

## Environment Variables

ตั้งค่า env vars สำหรับ Airflow callbacks:

```bash
# /opt/airflow/dags/etl-template/.env
GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/XXX/messages?key=YYY&token=ZZZ
```

หรือตั้งผ่าน systemd service file:

```bash
# /etc/systemd/system/airflow-worker.service
Environment="GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/..."
```

---

## Systemd Service Files

**Scheduler:**
```ini
# /etc/systemd/system/airflow-scheduler.service
[Unit]
Description=Airflow Scheduler
After=network.target postgresql.service

[Service]
User=airflow
Group=airflow
Type=simple
Environment="AIRFLOW_HOME=/opt/airflow"
ExecStart=/usr/local/bin/airflow scheduler
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

**Worker (CeleryExecutor):**
```ini
# /etc/systemd/system/airflow-worker.service
[Unit]
Description=Airflow Celery Worker
After=network.target redis.service

[Service]
User=airflow
Group=airflow
Type=simple
Environment="AIRFLOW_HOME=/opt/airflow"
Environment="GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/..."
ExecStart=/usr/local/bin/airflow celery worker --concurrency 8 --queues default,etl
Restart=always
RestartSec=10
# Memory limit — 10 GB for worker (เหลือให้ scheduler/webserver/redis)
MemoryMax=10G
# CPU affinity — pin to core 2-5 (core 0-1 = scheduler, 6 = webserver, 7 = redis)
# CPUAffinity=2-5

[Install]
WantedBy=multi-user.target
```

**Webserver:**
```ini
# /etc/systemd/system/airflow-webserver.service
[Unit]
Description=Airflow Webserver
After=network.target

[Service]
User=airflow
Group=airflow
Type=simple
Environment="AIRFLOW_HOME=/opt/airflow"
ExecStart=/usr/local/bin/airflow webserver --port 8080 --workers 4
Restart=always

[Install]
WantedBy=multi-user.target
```

---

## Performance Tuning Checklist

| ปัญหา | แก้ไข |
|---|---|
| DAG ใหม่ไม่ขึ้น / ขึ้นช้า | ลด `dag_dir_list_interval` (30s), ลด `min_file_process_interval` (10s) |
| Scheduler CPU สูง | เพิ่ม `min_file_process_interval`, ใส่ `.airflowignore` |
| Task queue backlog | เพิ่ม `parallelism`, เพิ่ม `worker_concurrency`, เพิ่ม worker nodes |
| Task ถูก mark zombie | เพิ่ม `scheduler_zombie_task_threshold` (600+) |
| OOM on worker | ลด `worker_concurrency`, เพิ่ม RAM, หรือแยก heavy jobs ไป queue อื่น |
| DB connection pool exhausted | เพิ่ม `sql_alchemy_pool_size` + `max_overflow` |

---

## Prerequisites

- [ ] `uv` installed บน Airflow server
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- [ ] `.airflowignore` สร้างแล้ว
- [ ] `.env` ตั้งค่าบน server (DB credentials, `GCHAT_WEBHOOK_URL`)
- [ ] Kerberos keytab accessible by Airflow worker (ถ้าใช้ GSSAPI)
- [ ] Jenkins pipeline deploy ไปที่ `/opt/airflow/dags/etl-template/`
- [ ] PostgreSQL metadata DB (ห้ามใช้ SQLite ใน production)
- [ ] systemd services enabled: scheduler, worker, webserver
