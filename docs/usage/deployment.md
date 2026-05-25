# Deployment Guide

Step-by-step deploy ETL template บน server ใหม่ — ทั้ง on-premise และ Docker

---

## Prerequisites

| Component | Version | ทำไม |
|---|---|---|
| Python | 3.11+ | Runtime |
| uv | latest | Package manager |
| Git | 2.x | Deploy + version control |
| PostgreSQL | 14+ | Airflow metadata DB + target |
| Redis | 6+ | Celery broker (ถ้าใช้ CeleryExecutor) |

---

## Option 1: On-premise (Git Pull)

เหมาะกับ: debug ง่าย, ไม่ต้อง build image, single server

### Step 1: Prepare Server

```bash
# สร้าง user
sudo useradd -m -s /bin/bash airflow
sudo su - airflow

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc

# Install Airflow (system-wide หรือ venv)
pip install apache-airflow[celery,redis,postgres]==2.10.*
```

### Step 2: Clone Project

```bash
# Clone ไปที่ dags directory
cd /opt/airflow/dags
git clone git@your-git-server:team/etl-template.git
cd etl-template

# Install dependencies
uv sync

# Setup env
cp .env.example .env
# แก้ไข .env ตาม environment
```

### Step 3: Configure .env

```bash
# /opt/airflow/dags/etl-template/.env
POSTGRES_HOST=pg-prod-server
POSTGRES_PORT=5432
POSTGRES_DB=data_warehouse
POSTGRES_USER=etl_user
POSTGRES_PASSWORD=<secret>

MSSQL_HOST=erp-server
MSSQL_PORT=1433
MSSQL_DB=erp_prod
MSSQL_USER=etl_reader
MSSQL_PASSWORD=<secret>

IMPALA_HOST=impala-server
IMPALA_PORT=21050
IMPALA_AUTH_MECHANISM=GSSAPI

HDFS_URL=http://namenode:9870
HDFS_AUTH_MECHANISM=GSSAPI
HDFS_KERBEROS_PRINCIPAL=airflow@REALM.COM

MINIO_ENDPOINT=minio-server:9000
MINIO_ACCESS_KEY=<key>
MINIO_SECRET_KEY=<secret>

GCHAT_WEBHOOK_URL=https://chat.googleapis.com/v1/spaces/XXX/messages?key=YYY&token=ZZZ
ETL_PROJECT_DIR=/opt/airflow/dags/etl-template
```

### Step 4: Verify

```bash
# ทดสอบ import + list jobs
cd /opt/airflow/dags/etl-template
uv run main.py --list

# ทดสอบ job
uv run main.py --job healthcheck
```

### Step 5: Airflow Config

```bash
# airflow.cfg — ตั้ง dags_folder
dags_folder = /opt/airflow/dags

# Initialize DB
airflow db init
airflow users create --role Admin --username admin --password admin \
    --email admin@example.com --firstname Admin --lastname User
```

### Step 6: Start Services

```bash
# Systemd services (ดู docs/usage/airflow.md สำหรับ service files เต็ม)
sudo systemctl enable --now airflow-scheduler
sudo systemctl enable --now airflow-worker
sudo systemctl enable --now airflow-webserver

# หรือ manual start
airflow scheduler &
airflow celery worker --concurrency 8 --queues default,etl &
airflow webserver --port 8080 &
```

### Step 7: Jenkins CI/CD

```
Jenkins Pipeline → git pull → /opt/airflow/dags/etl-template/
```

ดู `Jenkinsfile` สำหรับ pipeline เต็ม — flow:
1. Checkout → Setup (uv sync) → Test (pytest) → Deploy (git pull via SSH)
2. แจ้ง Google Chat เมื่อ success/failure

---

## Option 2: Docker (Harbor)

เหมาะกับ: reproducible builds, isolated env, DockerOperator

### Step 1: Build Image

```bash
# Build ETL image
docker build -t harbor.company.com/etl/etl-template:latest .
docker build -t harbor.company.com/etl/etl-template:v1.0 .

# Push to Harbor
docker push harbor.company.com/etl/etl-template:latest
docker push harbor.company.com/etl/etl-template:v1.0
```

### Step 2: Test Image

```bash
# Test run
docker run --env-file .env harbor.company.com/etl/etl-template:latest --list
docker run --env-file .env harbor.company.com/etl/etl-template:latest --job healthcheck
```

### Step 3: Airflow DockerOperator

```python
from airflow.providers.docker.operators.docker import DockerOperator

DockerOperator(
    task_id="sync_sales",
    image="harbor.company.com/etl/etl-template:latest",
    command="--job sync_sales",
    environment={
        "POSTGRES_HOST": "{{ var.value.pg_host }}",
        # ... หรือ mount .env
    },
    docker_url="unix:///var/run/docker.sock",
    auto_remove=True,
    network_mode="host",
)
```

### Jenkins CI/CD (Docker)

ดู `Jenkinsfile.docker` — flow:
1. Checkout → Test → Build image → Push Harbor → SSH deploy (docker compose pull)
2. แจ้ง Google Chat เมื่อ success/failure

---

## Option 3: Docker Compose (Full Stack)

เหมาะกับ: single-node full Airflow stack, dev/staging

### Step 1: Setup

```bash
git clone git@your-git-server:team/etl-template.git
cd etl-template

cp .env.example .env
# แก้ .env ตาม environment
```

### Step 2: Start

```bash
docker compose -f docker-compose.airflow.yml up -d

# URLs:
# Airflow UI:  http://localhost:8080 (admin/admin)
# Flower:      http://localhost:5555
```

### Step 3: Verify

```bash
# Check services
docker compose -f docker-compose.airflow.yml ps

# Check logs
docker compose -f docker-compose.airflow.yml logs scheduler
docker compose -f docker-compose.airflow.yml logs worker
```

### Update (deploy new code)

```bash
docker compose -f docker-compose.airflow.yml pull
docker compose -f docker-compose.airflow.yml up -d
```

---

## Server Directory Structure

```
/opt/airflow/
├── airflow.cfg
├── dags/
│   └── etl-template/            ← project
│       ├── main.py
│       ├── connector/
│       ├── jobs/
│       ├── utils/
│       ├── dags/                 ← Airflow scans here
│       │   ├── example_dag.py
│       │   └── prod_daily_etl.py
│       ├── .env                  ← secrets (ไม่ commit)
│       ├── .airflowignore        ← exclude non-DAG files
│       └── pyproject.toml
├── logs/
└── plugins/
```

---

## Kerberos Setup (ถ้าใช้ Impala/HDFS)

```bash
# 1. Install Kerberos client
sudo apt-get install krb5-user

# 2. Configure /etc/krb5.conf
[realms]
REALM.COM = {
    kdc = kdc-server.realm.com
    admin_server = kdc-server.realm.com
}

# 3. Get keytab (จาก admin)
# Copy keytab ไปที่ server
sudo cp airflow.keytab /etc/security/keytabs/

# 4. Test kinit
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
klist  # ดู ticket

# 5. Auto-renew (crontab)
# ทุก 8 ชม. (ticket ปกติ valid 24 ชม.)
0 */8 * * * kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
```

---

## Secrets Management

### ❌ ห้ามทำ

```bash
# ห้าม commit .env / secrets
# ห้าม hardcode password ใน code
# ห้ามใส่ secret ใน DAG file
```

### ✅ วิธีที่ถูก

| วิธี | เมื่อไหร่ | How |
|---|---|---|
| `.env` file on server | Simple, single server | `chmod 600 .env`, owned by airflow user |
| Environment variable (systemd) | On-premise production | `Environment=` ใน service file |
| Airflow Variables/Connections | Multi-DAG, team-wide | Airflow UI หรือ `airflow variables set` |
| Vault/SOPS (advanced) | Enterprise, multi-env | HashiCorp Vault, Mozilla SOPS |

```bash
# ตั้ง file permission
chmod 600 /opt/airflow/dags/etl-template/.env
chown airflow:airflow /opt/airflow/dags/etl-template/.env
```

---

## Health Check

```bash
# ดู Airflow services
systemctl status airflow-scheduler
systemctl status airflow-worker
systemctl status airflow-webserver

# ดู DAG parsed สำเร็จ
airflow dags list | grep etl

# ดู task queue
airflow celery worker --pid /tmp/worker.pid
celery -A airflow.executors.celery_executor.app inspect active

# Test connectivity
uv run main.py --job healthcheck
```

---

## Troubleshooting

| ปัญหา | สาเหตุ | แก้ไข |
|---|---|---|
| DAG ไม่ขึ้นใน UI | `.airflowignore` ไม่ถูก, parse error | `python dags/my_dag.py` ดู error |
| Import error ใน DAG | uv sync ไม่ครบ | `cd etl-template && uv sync` |
| Connection refused (DB) | Firewall, wrong host/port | `telnet host port` ทดสอบ |
| Kerberos expired | Ticket หมดอายุ | `kinit -kt keytab principal` |
| Worker OOM | Data ใหญ่เกิน RAM | ลด `worker_concurrency`, ใช้ chunked processing |
| Permission denied (.env) | File ownership ไม่ถูก | `chown airflow:airflow .env` |
| Docker image not found | Registry auth / tag ผิด | `docker login harbor.company.com` |

---

## Upgrade Workflow

```bash
# On-premise
cd /opt/airflow/dags/etl-template
git pull origin main
uv sync  # ถ้า dependencies เปลี่ยน

# Docker
docker compose -f docker-compose.airflow.yml pull
docker compose -f docker-compose.airflow.yml up -d

# Verify
uv run main.py --list
airflow dags list
```

---

## Summary

| Deploy Method | ข้อดี | ข้อเสีย | เหมาะกับ |
|---|---|---|---|
| **On-premise (git pull)** | Debug ง่าย, deploy เร็ว | ไม่ isolated, dependency conflict | Dev/staging |
| **Docker (Harbor)** | Reproducible, isolated | Build time, image size | Production |
| **Docker Compose** | Full stack 1 command | Single node | Dev/small prod |
