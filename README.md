# ETL Template

Python 3.11 + `uv` — deploy on-premise (Airflow + Jenkins) หรือ Docker

## Quick Start

```bash
uv sync                        # install dependencies
cp .env.example .env           # configure
uv run main.py --list          # list jobs
uv run main.py --job data_sync # run job
uv run pytest                  # test
```

## Project Structure

```
etl-template/
├── main.py                         # Entry point (--job, --list, --log-level)
├── connector/
│   ├── database/                   # postgres, mssql, impala, qdrant
│   ├── storage/                    # minio, hdfs
│   └── nats.py
├── jobs/
│   ├── base.py                     # BaseJob ABC
│   ├── registry.py                 # JOB_REGISTRY
│   └── example/                    # example jobs
├── utils/
│   ├── config.py                   # Settings (.env → pydantic-settings)
│   ├── schema.py                   # CREATE TABLE generators
│   ├── upsert.py                   # Upsert/incremental operations
│   ├── notify.py                   # Google Chat callbacks
│   └── ...                         # logger, retry, timer, converter, file_handler
├── dags/                           # Airflow DAG
├── docs/                           # Detailed documentation
├── Dockerfile                      # ETL job image (docker run --job xxx)
├── Dockerfile.airflow              # Airflow + Celery image
├── docker-compose.airflow.yml      # Full Airflow stack
├── Jenkinsfile                     # CI/CD: git pull deploy
└── Jenkinsfile.docker              # CI/CD: Docker + Harbor
```

## Adding a Job

```python
# jobs/my_job.py
from jobs.base import BaseJob
from utils.config import Settings

class MyJob(BaseJob):
    name = "my_job"
    def run(self, settings: Settings):
        ...
```

```python
# jobs/registry.py
JOB_REGISTRY["my_job"] = MyJob
```

## Local Development

```bash
uv sync && cp .env.example .env
uv run main.py --job data_sync     # รันตรง ไม่ต้อง Docker
```

**Test Airflow locally:**
```bash
docker compose -f docker-compose.airflow.yml up -d
# http://localhost:8080 (admin/admin) | Flower: http://localhost:5555
```

> Source mount เป็น volume — แก้ code เห็นผลเลย ไม่ต้อง rebuild

### Dockerfile ต่างกันยังไง?

| File | Purpose | When |
|---|---|---|
| `Dockerfile` | ETL job image (`docker run --job xxx`) | Deploy ผ่าน Harbor / DockerOperator |
| `Dockerfile.airflow` | Airflow + Celery + ETL | `docker-compose.airflow.yml` |

## Deploy

| วิธี | เหมาะกับ |
|---|---|
| **On-premise** — Jenkins git pull → Airflow `BashOperator` | Debug ง่าย, ไม่ต้อง build image |
| **Docker** — `Dockerfile` → Harbor → `DockerOperator` | Isolated, reproducible |
| **Compose** — `docker-compose.airflow.yml` บน VM | Single node full stack |

**On-premise flow:**
```
Jenkins → git pull → /opt/airflow/dags/etl-template/
Airflow → BashOperator → cd ... && uv run main.py --job xxx
```

**Docker flow:**
```bash
docker build -t harbor.company.com/etl/etl-template:latest .
docker run --env-file .env etl-template --job data_sync
```

## Documentation

### Usage (ใช้งาน template)

| Doc | เนื้อหา |
|---|---|
| [docs/usage/connectors.md](docs/usage/connectors.md) | Connectors ทุกตัว — PG, MSSQL, Impala, Qdrant, HDFS, MinIO, NATS |
| [docs/usage/deployment.md](docs/usage/deployment.md) | Deploy step-by-step — on-premise, Docker, Compose, Kerberos, secrets |
| [docs/usage/airflow.md](docs/usage/airflow.md) | Airflow config, scheduler, worker, Redis, systemd (prod 8C/16GB) |
| [docs/usage/operators.md](docs/usage/operators.md) | DAG template, BashOperator, DockerOperator, GChat callbacks, retry |
| [docs/usage/schema.md](docs/usage/schema.md) | CREATE TABLE generators (postgres/mssql/impala/kudu/iceberg) |
| [docs/usage/upsert.md](docs/usage/upsert.md) | Upsert/incremental functions, batch sizes, examples |
| [docs/usage/scaling.md](docs/usage/scaling.md) | DB insert scaling — library เลือกตาม volume (PG/MSSQL/Kudu/Iceberg) |

### Guide (แนวทาง/ความรู้)

| Doc | เนื้อหา |
|---|---|
| [docs/guide/pools-workers.md](docs/guide/pools-workers.md) | Pools (concurrency limit) & Workers (queue routing, autoscale, health) |
| [docs/guide/dag-best-practices.md](docs/guide/dag-best-practices.md) | DAG file best practices — import, parse, naming, timeout, XCom |
| [docs/guide/orchestration-patterns.md](docs/guide/orchestration-patterns.md) | Orchestration — Sensors, Trigger DAG, Dataset scheduling, Branching |
| [docs/guide/processing.md](docs/guide/processing.md) | Processing engines — pandas vs polars vs dask vs Spark |
| [docs/guide/patterns.md](docs/guide/patterns.md) | Pipeline patterns — CDC, SCD, Incremental, Idempotency, Backfill |
| [docs/guide/error-handling.md](docs/guide/error-handling.md) | Error handling — Retry, DLQ, Circuit breaker, Graceful shutdown |
| [docs/guide/sql-vs-python.md](docs/guide/sql-vs-python.md) | SQL vs Python — เมื่อไหร่ใช้อะไร, pushdown, hybrid pattern |
| [docs/guide/connectors-comparison.md](docs/guide/connectors-comparison.md) | ODBC vs JDBC vs Native — เปรียบเทียบ, benchmark, เมื่อไหร่ใช้อะไร |

## CI/CD

- **Jenkinsfile** — git pull deploy → `/opt/airflow/dags/etl-template`
- **Jenkinsfile.docker** — build → push Harbor → SSH compose pull
- Google Chat notification on success/failure
