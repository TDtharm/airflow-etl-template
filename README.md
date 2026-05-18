# ETL Template

ETL pipeline template — ใช้ `uv` + Python 3.11, รองรับ connectors หลายตัว, deploy ผ่าน Jenkins + Airflow

## Connectors

| Service    | Library         | Port  | Path                        |
|------------|-----------------|-------|-----------------------------|
| PostgreSQL | `psycopg2`      | 5432  | `connector/database/postgres.py` |
| MSSQL      | `pymssql`       | 1433  | `connector/database/mssql.py`    |
| Impala     | `impyla`        | 21050 | `connector/database/impala.py`   |
| Qdrant     | `qdrant-client` | 6333  | `connector/database/qdrant.py`   |
| MinIO      | `minio`         | 9000  | `connector/storage/minio.py`     |
| NATS       | `nats-py`       | 4222  | `connector/nats.py`              |

## Project Structure

```
etl-template/
├── main.py                     # Entry point (--job, --list, --log-level)
├── connector/
│   ├── database/               # DB connectors (postgres, mssql, impala, qdrant)
│   ├── storage/                # Object storage (minio)
│   └── nats.py                 # Messaging (nats)
├── job/
│   ├── base.py                 # BaseJob ABC
│   ├── registry.py             # JOB_REGISTRY mapping
│   ├── data_sync.py            # Example job
│   ├── backup.py               # Example job
│   └── healthcheck.py          # Example job
├── dags/
│   └── example_dag.py          # Airflow DAG example (BashOperator → main.py)
├── model/
│   └── base.py                 # Pydantic base model
├── utils/
│   ├── config.py               # Settings (pydantic-settings, .env)
│   ├── logger.py               # Loguru logger
│   ├── schema.py               # CREATE TABLE generators (postgres/mssql/impala/kudu)
│   ├── upsert.py               # Upsert operations (postgres/mssql/impala)
│   ├── converter.py            # DataFrame ↔ JSON/CSV converters
│   ├── file_handler.py         # Read/write JSON, CSV, Parquet files
│   ├── retry.py                # @retry decorator (exponential backoff)
│   └── timer.py                # @timer decorator
├── tests/                      # pytest tests
├── Dockerfile                  # python:3.11-slim + uv
├── docker-compose.yml          # Local dev services
├── Jenkinsfile                 # CI/CD: git pull deploy
├── Jenkinsfile.docker          # CI/CD: Docker + Harbor deploy
├── .env.example                # Environment variable template
└── pyproject.toml              # Dependencies (uv)
```

## Quick Start

```bash
# install dependencies
uv sync

# copy and configure environment
cp .env.example .env

# list available jobs
uv run main.py --list

# run a job
uv run main.py --job data_sync

# run tests
uv run pytest
```

## Adding a New Job

1. สร้างไฟล์ใน `job/` — extend `BaseJob`:

```python
# job/my_job.py
from job.base import BaseJob
from utils.config import Settings

class MyJob(BaseJob):
    def run(self, settings: Settings):
        # your ETL logic here
        pass
```

2. ลงทะเบียนใน `job/registry.py`:

```python
from job.my_job import MyJob
JOB_REGISTRY["my_job"] = MyJob
```

3. รัน:

```bash
uv run main.py --job my_job
```

## Schema & Upsert

```python
import pandas as pd
from utils.schema import create_table_postgres
from utils.upsert import upsert_postgres
from connector.database import PostgresConnector

df = pd.DataFrame({"id": [1], "name": ["example"]})

# Generate CREATE TABLE (auto-adds insert_date UTC + insert_by)
print(create_table_postgres(df, "my_table", unique_columns=["id"]))

# Upsert
with PostgresConnector(host, port, db, user, pwd) as pg:
    upsert_postgres(pg.conn, df, "my_table", conflict_columns=["id"])
```

รองรับ: `create_table_postgres`, `create_table_mssql`, `create_table_impala`, `create_table_kudu`
Upsert: `upsert_postgres` (ON CONFLICT), `upsert_mssql` (MERGE), `upsert_impala` (UPSERT INTO)

## Airflow DAG

DAG ใช้ `BashOperator` เรียก `main.py --job xxx` — ดูตัวอย่างที่ `dags/example_dag.py`

## Docker

```bash
# local dev (postgres, mssql, qdrant, minio, nats)
docker compose up -d

# build image
docker build -t etl-template .

# run
docker run --env-file .env etl-template --job data_sync
```

## CI/CD

- **Jenkinsfile** — git pull deploy to `/opt/airflow/dags/`
- **Jenkinsfile.docker** — build → push Harbor → SSH compose pull
- ทั้ง 2 มี Google Chat notification
# airflow-etl-template
