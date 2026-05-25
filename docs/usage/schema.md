# Schema — CREATE TABLE Generators

สร้าง DDL statement จาก pandas DataFrame อัตโนมัติ — รองรับ PostgreSQL, MSSQL, Impala (Parquet), Kudu, Iceberg

ทุก function เพิ่ม `insert_date` (TIMESTAMP UTC) และ `insert_by` (VARCHAR/STRING) ให้อัตโนมัติ

```python
from utils.schema import (
    create_table_postgres,
    create_table_mssql,
    create_table_impala,
    create_table_kudu,
    create_table_iceberg,
)
```

---

## Type Mapping

| pandas dtype | PostgreSQL | MSSQL | Impala/Kudu | Iceberg |
|---|---|---|---|---|
| `int64` | BIGINT | BIGINT | BIGINT | BIGINT |
| `int32` | INTEGER | INT | INT | INT |
| `float64` | DOUBLE PRECISION | FLOAT | FLOAT | DOUBLE |
| `float32` | FLOAT | REAL | FLOAT | FLOAT |
| `bool` | BOOLEAN | BIT | BOOLEAN | BOOLEAN |
| `datetime64` | TIMESTAMP | DATETIME2 | TIMESTAMP | TIMESTAMP |
| `object` | VARCHAR | NVARCHAR(MAX) | STRING | STRING |

> column ชื่อ `geom` / `geometry` จะถูก map เป็น `GEOMETRY` อัตโนมัติ (PostgreSQL/PostGIS)

---

## PostgreSQL

```python
create_table_postgres(
    df,
    table="my_table",
    schema="public",             # default "public"
    database=None,               # ถ้าใส่จะเป็น database.schema.table
    if_not_exists=True,
    column_overrides=None,       # {"lat": "NUMERIC(10,6)", "geom": "GEOMETRY(Point,4326)"}
    unique_columns=None,         # ["id"] → UNIQUE constraint (สำหรับ upsert ON CONFLICT)
)
```

**Example:**

```python
import pandas as pd
from utils.schema import create_table_postgres

df = pd.DataFrame({"id": [1], "name": ["test"], "lat": [13.7]})
ddl = create_table_postgres(df, "locations", unique_columns=["id"], column_overrides={"lat": "NUMERIC(10,6)"})
print(ddl)
```

Output:
```sql
CREATE TABLE IF NOT EXISTS public.locations (
    id BIGINT,
    name VARCHAR,
    lat NUMERIC(10,6),
    insert_date TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC'),
    insert_by VARCHAR,
    UNIQUE (id)
);
```

---

## MSSQL

```python
create_table_mssql(
    df,
    table="my_table",
    schema="dbo",                # default "dbo"
    if_not_exists=True,          # wraps with IF NOT EXISTS check
)
```

Output:
```sql
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'my_table')
CREATE TABLE dbo.my_table (
    id BIGINT,
    name NVARCHAR(MAX),
    insert_date DATETIME2,
    insert_by NVARCHAR(MAX)
);
```

---

## Impala (Parquet)

```python
create_table_impala(
    df,
    table="events",
    database="analytics",        # default "default"
    stored_as="PARQUET",         # PARQUET, ORC, TEXTFILE
    external=True,               # EXTERNAL TABLE (default True)
    partition_columns=None,      # {"year": "INT", "month": "INT"}
)
```

**Partition example:**

```python
create_table_impala(
    df,
    table="logs",
    database="raw",
    partition_columns={"year": "INT", "month": "INT"},
)
```

Output:
```sql
CREATE EXTERNAL TABLE IF NOT EXISTS raw.logs (
    id BIGINT,
    message STRING,
    insert_date TIMESTAMP,
    insert_by STRING
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;
```

> Partition columns จะถูก **remove** จาก column list อัตโนมัติ (ตาม Hive convention)

---

## Kudu

```python
create_table_kudu(
    df,
    table="realtime_data",
    database="default",
    primary_key_columns=["id"],         # required: กำหนด PRIMARY KEY
    kudu_master="master1:7051",         # Kudu master address
    kudu_replicas=3,                    # จำนวน replica
    hash_partition_columns=["id"],      # HASH partition columns
    hash_partitions=8,                  # จำนวน hash buckets
    range_partition_column=None,        # RANGE partition column (optional)
)
```

**Example:**

```python
create_table_kudu(
    df,
    table="transactions",
    database="finance",
    primary_key_columns=["txn_id", "txn_date"],
    kudu_master="kudu-master.prod:7051",
    hash_partition_columns=["txn_id"],
    hash_partitions=16,
    range_partition_column="txn_date",
)
```

Output:
```sql
CREATE TABLE IF NOT EXISTS finance.transactions (
    txn_id BIGINT,
    txn_date TIMESTAMP,
    amount FLOAT,
    insert_date TIMESTAMP,
    insert_by STRING,
    PRIMARY KEY (txn_id, txn_date)
)
PARTITION BY HASH (txn_id) PARTITIONS 16,
    RANGE (txn_date) ()
STORED AS KUDU
TBLPROPERTIES(
    'kudu.master_addresses' = 'kudu-master.prod:7051',
    'kudu.num_tablet_replicas' = '3'
);
```

> Primary key columns จะถูก **reorder** ไปอยู่ข้างหน้าอัตโนมัติ

---

## Iceberg (CDP 7.1.3+)

```python
create_table_iceberg(
    df,
    table="events",
    database="analytics",
    partition_spec=None,           # ["year(insert_date)", "bucket(8, id)"]
    tblproperties=None,            # {"write.format.default": "parquet"}
    external=False,
)
```

**Partition transforms ที่รองรับ:**
- `year(col)`, `month(col)`, `day(col)`, `hour(col)` — temporal
- `bucket(N, col)` — hash bucket
- `truncate(N, col)` — string prefix

**Example:**

```python
create_table_iceberg(
    df,
    table="page_views",
    database="web",
    partition_spec=["year(insert_date)", "bucket(16, user_id)"],
    tblproperties={"write.format.default": "parquet", "format-version": "2"},
)
```

Output:
```sql
CREATE TABLE IF NOT EXISTS web.page_views (
    user_id BIGINT,
    url STRING,
    insert_date TIMESTAMP,
    insert_by STRING
)
PARTITION BY SPEC (year(insert_date), bucket(16, user_id))
STORED AS ICEBERG
TBLPROPERTIES(
    'write.format.default' = 'parquet',
    'format-version' = '2'
);
```

> Iceberg ใช้ **hidden partitioning** — partition columns ยังอยู่ใน column list (ต่างจาก Hive/Impala)

---

## column_overrides

ใช้เมื่อต้องการ override type ที่ auto-detect จาก DataFrame:

```python
create_table_postgres(
    df,
    "geo_data",
    column_overrides={
        "lat": "NUMERIC(10,6)",
        "lng": "NUMERIC(10,6)",
        "geom": "GEOMETRY(Point, 4326)",
        "metadata": "JSONB",
    },
)
```
