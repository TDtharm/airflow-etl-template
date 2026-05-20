# Connectors — วิธีใช้งาน

วิธีใช้ connector ทุกตัวใน template — Database, Storage, Messaging

---

## Architecture

```
connector/
├── database/
│   ├── postgres.py     # PostgreSQL (psycopg2)
│   ├── mssql.py        # Microsoft SQL Server (pymssql)
│   ├── impala.py       # Apache Impala/Kudu (impyla) — PLAIN/LDAP/Kerberos
│   └── qdrant.py       # Qdrant vector DB
├── storage/
│   ├── hdfs.py         # HDFS WebHDFS — PLAIN/LDAP/Kerberos
│   └── minio.py        # MinIO / S3-compatible
└── nats.py             # NATS messaging (pub/sub)
```

ทุก connector ใช้ pattern เดียวกัน:
- `__init__()` → config
- `connect()` → establish connection
- `close()` → cleanup
- Context manager (`with ... as`) → auto connect/close

---

## PostgreSQL

```python
from connector.database.postgres import PostgresConnector

# Context manager (recommended)
with PostgresConnector(
    host="localhost",
    port=5432,
    database="mydb",
    user="postgres",
    password="secret",
) as pg:
    # Query
    rows = pg.fetch_all("SELECT * FROM users WHERE active = %s", (True,))
    row = pg.fetch_one("SELECT COUNT(*) as cnt FROM users")

    # Execute
    pg.execute("UPDATE users SET active = false WHERE id = %s", (123,))

    # ใช้ raw connection กับ pandas
    import pandas as pd
    df = pd.read_sql("SELECT * FROM users", pg.conn)

    # ใช้กับ upsert functions
    from utils.upsert import upsert_postgres
    upsert_postgres(pg.conn, df, "target_table", conflict_columns=["id"])
```

### .env

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=secret
```

### ใช้กับ Settings

```python
from utils.config import Settings

settings = Settings()
with PostgresConnector(
    host=settings.postgres_host,
    port=settings.postgres_port,
    database=settings.postgres_db,
    user=settings.postgres_user,
    password=settings.postgres_password,
) as pg:
    ...
```

### Methods

| Method | Return | Description |
|---|---|---|
| `fetch_all(sql, params)` | `list[dict]` | Query ทุก row (RealDictCursor) |
| `fetch_one(sql, params)` | `dict \| None` | Query 1 row |
| `execute(sql, params)` | `None` | INSERT/UPDATE/DELETE + auto commit |
| `.conn` | `psycopg2.connection` | Raw connection (สำหรับ pandas/upsert) |

---

## MSSQL

```python
from connector.database.mssql import MSSQLConnector

with MSSQLConnector(
    host="mssql-server",
    port=1433,
    database="erp",
    user="sa",
    password="secret",
) as mssql:
    rows = mssql.fetch_all("SELECT TOP 100 * FROM dbo.sales")
    row = mssql.fetch_one("SELECT COUNT(*) as cnt FROM dbo.sales")
    mssql.execute("UPDATE dbo.sales SET status = %s WHERE id = %s", ("done", 1))

    # pandas
    import pandas as pd
    df = pd.read_sql("SELECT * FROM dbo.sales WHERE dt >= '2025-01-01'", mssql.conn)

    # upsert
    from utils.upsert import upsert_mssql
    upsert_mssql(mssql.conn, df, "dbo.target", conflict_columns=["id"])
```

### .env

```bash
MSSQL_HOST=localhost
MSSQL_PORT=1433
MSSQL_DB=master
MSSQL_USER=sa
MSSQL_PASSWORD=secret
```

### Methods

เหมือน PostgresConnector: `fetch_all`, `fetch_one`, `execute`, `.conn`

---

## Impala/Kudu

รองรับ 3 auth modes: **PLAIN** (no auth), **LDAP**, **GSSAPI** (Kerberos)

### PLAIN (no auth — dev)

```python
from connector.database.impala import ImpalaConnector

with ImpalaConnector(
    host="impala-server",
    port=21050,
    database="default",
    auth_mechanism="PLAIN",
) as impala:
    rows = impala.fetch_all("SELECT * FROM my_table LIMIT 100")
```

### LDAP

```python
with ImpalaConnector(
    host="impala-server",
    port=21050,
    database="dwh",
    auth_mechanism="LDAP",
    user="etl_user",
    password="ldap_password",
    use_ssl=True,
    ca_cert="/etc/ssl/certs/ca-cert.pem",
) as impala:
    rows = impala.fetch_all("SELECT * FROM fact_sales")
```

### Kerberos (GSSAPI)

```bash
# ต้อง kinit ก่อนรัน
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
```

```python
with ImpalaConnector(
    host="impala-server",
    port=21050,
    database="dwh",
    auth_mechanism="GSSAPI",
    kerberos_service_name="impala",
    use_ssl=True,
) as impala:
    rows = impala.fetch_all("SELECT * FROM fact_sales")

    # upsert Kudu table
    from utils.upsert import upsert_impala
    upsert_impala(impala.conn, df, "kudu_table", conflict_columns=["id"])

    # upsert Iceberg table
    from utils.upsert import upsert_iceberg
    upsert_iceberg(impala.conn, df, "iceberg_table", conflict_columns=["id"])
```

### .env

```bash
IMPALA_HOST=localhost
IMPALA_PORT=21050
IMPALA_DB=default
IMPALA_USER=
IMPALA_PASSWORD=
IMPALA_AUTH_MECHANISM=PLAIN       # PLAIN, LDAP, GSSAPI
IMPALA_USE_SSL=false
IMPALA_KERBEROS_SERVICE_NAME=impala
IMPALA_CA_CERT=
```

### Methods

| Method | Return | Description |
|---|---|---|
| `fetch_all(sql, params)` | `list[dict]` | Query ทุก row |
| `fetch_one(sql, params)` | `dict \| None` | Query 1 row |
| `execute(sql, params)` | `None` | DDL/DML (ไม่ auto commit — Impala ไม่มี transaction) |
| `.conn` | `impyla connection` | Raw connection |

---

## Qdrant (Vector DB)

```python
from connector.database.qdrant import QdrantConnector

with QdrantConnector(
    host="localhost",
    port=6333,
    api_key="secret",
) as qdrant:
    # ใช้ qdrant_client API
    qdrant.client.create_collection(...)
    qdrant.client.upsert(...)
    results = qdrant.client.search(...)
```

### .env

```bash
QDRANT_HOST=localhost
QDRANT_PORT=6333
QDRANT_API_KEY=
```

---

## HDFS (WebHDFS)

รองรับ 3 auth modes: **PLAIN**, **LDAP**, **GSSAPI** (Kerberos)

### PLAIN (no auth)

```python
from connector.storage.hdfs import HDFSConnector

with HDFSConnector(
    url="http://namenode:9870",
    user="hdfs",
    auth_mechanism="PLAIN",
    root="/",
) as hdfs:
    # Upload
    hdfs.upload_file("/data/output/report.parquet", "/tmp/local_report.parquet")
    hdfs.upload_bytes("/data/output/data.csv", csv_bytes)

    # Download
    data = hdfs.download_bytes("/data/input/source.csv")
    hdfs.download_file("/data/input/source.parquet", "/tmp/local.parquet")

    # List / Status
    files = hdfs.list_dir("/data/output/")
    info = hdfs.status("/data/output/report.parquet")

    # Create directory
    hdfs.makedirs("/data/output/2025/01/")

    # Delete
    hdfs.delete("/data/tmp/old_file.csv")
    hdfs.delete("/data/tmp/old_dir", recursive=True)
```

### LDAP

```python
with HDFSConnector(
    url="http://namenode:9870",
    user="etl_user",
    auth_mechanism="LDAP",
    password="ldap_password",
) as hdfs:
    hdfs.upload_file("/data/output/report.parquet", "local.parquet")
```

### Kerberos

```bash
# kinit ก่อน
kinit -kt /etc/security/keytabs/airflow.keytab airflow@REALM.COM
```

```python
with HDFSConnector(
    url="https://namenode:9871",  # HTTPS สำหรับ Kerberos
    auth_mechanism="GSSAPI",
    kerberos_principal="airflow@REALM.COM",
) as hdfs:
    hdfs.upload_file("/data/output/report.parquet", "local.parquet")
```

### .env

```bash
HDFS_URL=http://localhost:9870
HDFS_USER=hdfs
HDFS_AUTH_MECHANISM=PLAIN         # PLAIN, LDAP, GSSAPI
HDFS_PASSWORD=                    # LDAP only
HDFS_KERBEROS_PRINCIPAL=          # GSSAPI only
HDFS_ROOT=/
```

### Methods

| Method | Description |
|---|---|
| `upload_file(hdfs_path, local_path)` | Upload local file to HDFS |
| `upload_bytes(hdfs_path, data)` | Upload bytes to HDFS |
| `download_file(hdfs_path, local_path)` | Download HDFS file to local |
| `download_bytes(hdfs_path)` → `bytes` | Download HDFS file as bytes |
| `list_dir(hdfs_path)` → `list[str]` | List directory contents |
| `makedirs(hdfs_path)` | Create directories recursively |
| `delete(hdfs_path, recursive)` | Delete file/directory |
| `status(hdfs_path)` → `dict \| None` | Get file status (size, mtime, etc.) |

---

## MinIO (S3-compatible)

```python
from connector.storage.minio import MinioConnector

with MinioConnector(
    endpoint="minio-server:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False,
) as minio:
    # Upload
    minio.upload_file("my-bucket", "reports/2025/jan.parquet", "/tmp/local.parquet")
    minio.upload_bytes("my-bucket", "data/output.csv", csv_bytes, content_type="text/csv")

    # Download
    data = minio.download_bytes("my-bucket", "data/input.csv")
    minio.download_file("my-bucket", "data/input.parquet", "/tmp/local.parquet")

    # List
    objects = minio.list_objects("my-bucket", prefix="reports/2025/")
    buckets = minio.list_buckets()

    # Delete
    minio.delete_object("my-bucket", "data/old_file.csv")
```

### Auto-create Bucket

```python
# ensure_bucket() ถูกเรียกอัตโนมัติตอน upload
# ถ้า bucket ไม่มี → สร้างให้
minio.upload_file("new-bucket", "file.txt", "/tmp/file.txt")
# new-bucket ถูกสร้างอัตโนมัติ
```

### .env

```bash
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=false
```

### Methods

| Method | Description |
|---|---|
| `upload_file(bucket, object_name, file_path)` | Upload local file |
| `upload_bytes(bucket, object_name, data, content_type)` | Upload bytes |
| `download_file(bucket, object_name, file_path)` | Download to local |
| `download_bytes(bucket, object_name)` → `bytes` | Download as bytes |
| `list_objects(bucket, prefix)` → `list[str]` | List objects |
| `list_buckets()` → `list[str]` | List all buckets |
| `delete_object(bucket, object_name)` | Delete object |
| `ensure_bucket(bucket)` | Create bucket if not exists |

---

## NATS (Messaging)

```python
from connector.nats import NatsConnector

with NatsConnector(
    servers="nats://localhost:4222",
    user="nats_user",
    password="secret",
) as nats:
    # Publish
    import json
    payload = json.dumps({"event": "etl_done", "table": "sales"}).encode()
    nats.publish("etl.events", payload)
```

### Async (advanced)

```python
import asyncio

async def main():
    connector = NatsConnector(servers="nats://localhost:4222")
    await connector.connect_async()

    # Publish
    await connector.publish_async("etl.events", b'{"status": "done"}')

    # Request-reply
    response = await connector.request_async("etl.status", b"ping", timeout=5.0)
    print(response)

    # Subscribe
    async def handler(msg):
        print(f"Received: {msg.data}")

    await connector.subscribe_async("etl.events", handler)

    await connector.close_async()

asyncio.run(main())
```

### .env

```bash
NATS_SERVERS=nats://localhost:4222
NATS_USER=
NATS_PASSWORD=
```

### Methods

| Method | Description |
|---|---|
| `publish(subject, payload)` | Publish message (sync) |
| `publish_async(subject, payload)` | Publish message (async) |
| `subscribe_async(subject, callback)` | Subscribe to subject |
| `request_async(subject, payload, timeout)` | Request-reply pattern |

---

## Usage ใน Job

### Pattern: ใช้ Settings + Connector

```python
from jobs.base import BaseJob
from utils.config import Settings
from connector.database.postgres import PostgresConnector
from connector.database.mssql import MSSQLConnector
from utils.upsert import upsert_postgres

class MySyncJob(BaseJob):
    name = "my_sync"

    def run(self, settings: Settings):
        # Source: MSSQL
        with MSSQLConnector(
            host=settings.mssql_host,
            port=settings.mssql_port,
            database=settings.mssql_db,
            user=settings.mssql_user,
            password=settings.mssql_password,
        ) as src:
            import pandas as pd
            df = pd.read_sql("SELECT * FROM dbo.sales WHERE dt >= '2025-01-01'", src.conn)

        # Target: PostgreSQL
        with PostgresConnector(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        ) as tgt:
            upsert_postgres(tgt.conn, df, "dwh.fact_sales", conflict_columns=["id"])
```

### Pattern: Multiple Connectors

```python
def run(self, settings: Settings):
    with MSSQLConnector(...) as src, \
         PostgresConnector(...) as pg, \
         HDFSConnector(...) as hdfs:

        df = pd.read_sql("SELECT * FROM source", src.conn)
        upsert_postgres(pg.conn, df, "target", conflict_columns=["id"])

        # Archive to HDFS
        parquet_bytes = df.to_parquet()
        hdfs.upload_bytes(f"/archive/{today}.parquet", parquet_bytes)
```

---

## Summary

| Connector | Library | Auth | Use Case |
|---|---|---|---|
| **PostgresConnector** | psycopg2 | user/password | Target DWH, metadata |
| **MSSQLConnector** | pymssql | user/password | Source ERP, legacy |
| **ImpalaConnector** | impyla | PLAIN/LDAP/Kerberos | Hadoop ecosystem |
| **QdrantConnector** | qdrant-client | API key | Vector search |
| **HDFSConnector** | hdfs (WebHDFS) | PLAIN/LDAP/Kerberos | File storage, archive |
| **MinioConnector** | minio | access/secret key | Object storage, S3 |
| **NatsConnector** | nats-py | user/password/token | Event messaging |
