# ETL Template

ETL pipeline template — Python 3.11 + `uv`, deploy on-premise ผ่าน Airflow + Jenkins

## Quick Start

```bash
uv sync                    # install dependencies
cp .env.example .env       # configure environment
uv run main.py --list      # list available jobs
uv run main.py --job data_sync
uv run pytest              # run tests
```

## Project Structure

```
etl-template/
├── main.py                 # Entry point (--job, --list, --log-level)
├── connector/
│   ├── database/           # postgres, mssql, impala, qdrant (LDAP/Kerberos)
│   ├── storage/            # minio, hdfs (LDAP/Kerberos)
│   └── nats.py             # messaging
├── job/
│   ├── base.py             # BaseJob ABC
│   ├── registry.py         # JOB_REGISTRY
│   └── example/            # example jobs (upsert, upload hdfs/minio)
├── dags/
│   └── example_dag.py      # Airflow DAG example
├── utils/
│   ├── config.py           # Settings (.env → pydantic-settings)
│   ├── schema.py           # CREATE TABLE (postgres/mssql/impala/kudu/iceberg)
│   ├── upsert.py           # Upsert/incremental (postgres/mssql/kudu/parquet/iceberg)
│   ├── logger.py           # Loguru
│   ├── retry.py            # @retry (exponential backoff)
│   ├── timer.py            # @timer
│   ├── converter.py        # DataFrame ↔ JSON/CSV
│   └── file_handler.py     # Read/write JSON, CSV, Parquet
├── tests/
├── Dockerfile
├── docker-compose.yml      # Local dev services
├── Jenkinsfile             # CI/CD: git pull deploy
└── Jenkinsfile.docker      # CI/CD: Docker + Harbor
```

## Adding a Job

```python
# job/my_job.py
from job.base import BaseJob
from utils.config import Settings

class MyJob(BaseJob):
    name = "my_job"
    def run(self, settings: Settings):
        ...
```

ลงทะเบียนใน `job/registry.py`:
```python
from job.my_job import MyJob
JOB_REGISTRY["my_job"] = MyJob
```

## Deploy — On-Premise (Git Pull)

วิธีที่ใช้กับ Airflow on-premise (bare metal / VM):

```
Server (Airflow Worker)
├── /opt/airflow/dags/
│   └── etl-template/        ← Jenkins git pull มาไว้ที่นี่
│       ├── dags/
│       │   └── example_dag.py   ← Airflow scan เจอไฟล์นี้
│       ├── main.py
│       ├── .env                 ← secrets อยู่บน server
│       └── ...
```

**Flow:**
1. Jenkins checkout + `uv sync` + pytest → SSH git pull ไปที่ Airflow server
2. Airflow scheduler scan `dags/example_dag.py`
3. DAG trigger ตาม schedule → `BashOperator`:
   ```bash
   cd /opt/airflow/dags/etl-template && uv run main.py --job data_sync
   ```
4. `main.py` อ่าน `.env` → เรียก job logic → เชื่อม DB/HDFS/MinIO

**ข้อดี:** debug ง่าย, ไม่ต้อง build image, แก้ code แล้ว git pull ได้เลย

## Deploy — Docker (Harbor)

```bash
# Build
docker build -t harbor.company.com/etl/etl-template:latest .
docker push harbor.company.com/etl/etl-template:latest

# Run (pass secrets at runtime)
docker run --env-file .env etl-template --job data_sync

# Kerberos — mount keytab
docker run --env-file .env \
  -v /etc/security/keytabs/user.keytab:/app/user.keytab \
  -e KRB5_CLIENT_KTNAME=/app/user.keytab \
  etl-template --job example_upload_hdfs
```

**Airflow + DockerOperator:**
```python
from airflow.providers.docker.operators.docker import DockerOperator

DockerOperator(
    task_id="data_sync",
    image="harbor.company.com/etl/etl-template:latest",
    command="--job data_sync",
    environment={"POSTGRES_HOST": "db.prod", "POSTGRES_PASSWORD": "xxx"},
    auto_remove=True,
)
```

## Airflow DAG Example

```python
# dags/example_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("etl_example", schedule="0 2 * * *", start_date=datetime(2025,1,1), catchup=False):
    BashOperator(
        task_id="data_sync",
        bash_command="cd /opt/airflow/dags/etl-template && uv run main.py --job data_sync",
    )
```

## Impala / HDFS — Kerberos & LDAP

```bash
# .env
IMPALA_AUTH_MECHANISM=GSSAPI          # หรือ LDAP
IMPALA_KERBEROS_SERVICE_NAME=impala
HDFS_AUTH_MECHANISM=GSSAPI
HDFS_KERBEROS_PRINCIPAL=user@REALM.COM

# kinit ก่อนรัน (on-premise)
kinit -kt /etc/security/keytabs/user.keytab user@REALM.COM
uv run main.py --job example_upload_hdfs
```

## Schema & Upsert

| Function | DB | Strategy |
|---|---|---|
| `upsert_postgres` | PostgreSQL | ON CONFLICT DO UPDATE |
| `insert_do_nothing_postgres` | PostgreSQL | ON CONFLICT DO NOTHING |
| `upsert_mssql` | MSSQL | MERGE |
| `upsert_impala` | Kudu | UPSERT INTO |
| `upsert_parquet_impala` | Parquet | Staging + INSERT OVERWRITE |
| `upsert_iceberg` | Iceberg | MERGE INTO (CDP 7.1.3+) |

> รายละเอียด parameters, examples, partition: [docs/schema.md](docs/schema.md) | [docs/upsert.md](docs/upsert.md)

## CI/CD (Jenkins)

- **Jenkinsfile** — git pull deploy → `/opt/airflow/dags/etl-template`
- **Jenkinsfile.docker** — build → push Harbor → SSH docker compose pull
- ทั้ง 2 pipeline มี Google Chat notification
# airflow-etl-template
