# Upsert & Incremental Insert

Upsert (INSERT or UPDATE) สำหรับ PostgreSQL, MSSQL, Impala/Kudu, Parquet, Iceberg

ทุก function เพิ่ม `insert_date` (UTC) และ `insert_by` ให้อัตโนมัติ

```python
from utils.upsert import (
    upsert_postgres,
    insert_do_nothing_postgres,
    upsert_mssql,
    upsert_impala,              # alias ของ upsert_kudu_impala
    upsert_parquet_impala,
    insert_incremental_parquet_impala,
    upsert_iceberg,
    insert_incremental_iceberg,
)
```

---

## Batch Sizes

| DB | Default | Reason |
|---|---|---|
| PostgreSQL | 5,000 | `execute_values` ส่ง 1 round-trip per batch — PG รับ VALUES ใหญ่ได้ |
| MSSQL | 500 | MERGE ทำ per-row, TDS packet ~4KB, batch เล็กลด lock time |
| Impala/Kudu | 2,000 | Kudu flush per batch, balance ระหว่าง RPC calls กับ memory |

---

## PostgreSQL — ON CONFLICT DO UPDATE

```python
upsert_postgres(
    conn,                          # psycopg2 connection
    df,                            # pandas DataFrame
    table="my_table",
    conflict_columns=["id"],       # UNIQUE columns สำหรับ ON CONFLICT
    schema="public",
    update_columns=None,           # ถ้า None จะ update ทุก column ที่ไม่ใช่ conflict
    insert_by="etl_user",
    batch_size=5000,
) -> int  # returns rows affected
```

**Strategy:** `INSERT ... ON CONFLICT (id) DO UPDATE SET ...`

ใช้ `psycopg2.extras.execute_values` — เร็วกว่า executemany ~10x

**Example:**

```python
from connector.database import PostgresConnector
from utils.upsert import upsert_postgres

with PostgresConnector(host, port, db, user, pwd) as pg:
    count = upsert_postgres(
        pg.conn, df, "users",
        conflict_columns=["user_id"],
        insert_by="sync_job",
    )
    print(f"Upserted {count} rows")
```

---

## PostgreSQL — ON CONFLICT DO NOTHING (Incremental)

```python
insert_do_nothing_postgres(
    conn,
    df,
    table="events",
    conflict_columns=["event_id"],
    schema="public",
    insert_by="etl_user",
    batch_size=5000,
) -> int
```

**Strategy:** `INSERT ... ON CONFLICT (event_id) DO NOTHING`

เหมาะสำหรับ append-only data (logs, events) — ข้ามแถวที่ซ้ำโดยไม่ update

---

## MSSQL — MERGE

```python
upsert_mssql(
    conn,                          # pymssql connection
    df,
    table="my_table",
    conflict_columns=["id"],       # JOIN condition สำหรับ MERGE
    schema="dbo",
    update_columns=None,
    insert_by="etl_user",
    batch_size=500,
) -> int
```

**Strategy:**
```sql
MERGE target AS target
USING (SELECT ...) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

**Example:**

```python
from connector.database import MSSQLConnector
from utils.upsert import upsert_mssql

with MSSQLConnector(host, port, db, user, pwd) as mssql:
    count = upsert_mssql(
        mssql.conn, df, "customers",
        conflict_columns=["customer_id"],
    )
```

---

## Impala/Kudu — UPSERT INTO

```python
upsert_impala(
    conn,                          # impyla connection
    df,
    table="realtime_data",
    database="default",
    insert_by="etl_user",
    batch_size=2000,
) -> int
```

**Strategy:** `UPSERT INTO table (cols) VALUES (...)` — Kudu native upsert

> ไม่ต้องระบุ conflict columns — Kudu ใช้ PRIMARY KEY ที่ตั้งไว้ตอนสร้างตาราง

**Example:**

```python
from connector.database import ImpalaConnector
from utils.upsert import upsert_impala

with ImpalaConnector(host, port, db, auth_mechanism="LDAP", user=user, password=pwd) as imp:
    count = upsert_impala(imp.conn, df, "sensor_data", database="iot")
```

---

## Impala/Parquet — Staging + INSERT OVERWRITE

Parquet เป็น immutable format — ไม่สามารถ UPDATE row ได้ ต้อง rewrite ทั้ง partition

```python
upsert_parquet_impala(
    conn,
    df,
    table="daily_summary",
    key_columns=["id", "date"],     # columns สำหรับ match (เหมือน conflict_columns)
    database="analytics",
    insert_by="etl_user",
) -> int
```

**Strategy:**
1. สร้าง staging table (clone target)
2. INSERT data ใหม่เข้า staging
3. `INSERT OVERWRITE target = staging UNION ALL (target LEFT ANTI JOIN staging)`
4. DROP staging

> Rewrite ทั้งตาราง — เหมาะกับตารางขนาดเล็ก-กลาง หรือ partitioned table

---

## Impala/Parquet — Incremental Insert

```python
insert_incremental_parquet_impala(
    conn,
    df,
    table="events",
    key_columns=["event_id"],
    database="raw",
    insert_by="etl_user",
) -> int
```

**Strategy:**
1. สร้าง staging table
2. INSERT data ใหม่เข้า staging
3. `INSERT INTO target SELECT FROM staging LEFT ANTI JOIN target` (เฉพาะ key ใหม่)
4. DROP staging

> เหมือน DO NOTHING — ไม่ update rows ที่มีอยู่แล้ว

---

## Iceberg — MERGE INTO (CDP 7.1.3+)

Native ACID — ไม่ต้อง rewrite ทั้งตาราง, row-level MERGE

```python
upsert_iceberg(
    conn,
    df,
    table="transactions",
    key_columns=["txn_id"],         # JOIN condition สำหรับ MERGE
    database="finance",
    update_columns=None,            # ถ้า None จะ update ทุก column ที่ไม่ใช่ key
    insert_by="etl_user",
    batch_size=2000,
) -> int
```

**Strategy:**
1. สร้าง staging table
2. INSERT data เข้า staging (batch)
3. `MERGE INTO target USING staging ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`
4. DROP staging

**Example:**

```python
from connector.database import ImpalaConnector
from utils.upsert import upsert_iceberg

with ImpalaConnector(host, port, db, auth_mechanism="GSSAPI") as imp:
    count = upsert_iceberg(
        imp.conn, df, "orders",
        key_columns=["order_id"],
        database="sales",
        insert_by="daily_etl",
    )
```

---

## Iceberg — Incremental Insert

```python
insert_incremental_iceberg(
    conn,
    df,
    table="events",
    key_columns=["event_id"],
    database="raw",
    insert_by="etl_user",
    batch_size=2000,
) -> int
```

**Strategy:** `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` (ไม่ update rows ที่มีอยู่)

---

## Summary Table

| Function | DB | Strategy | ต้องระบุ key |
|---|---|---|---|
| `upsert_postgres` | PostgreSQL | ON CONFLICT DO UPDATE | `conflict_columns` |
| `insert_do_nothing_postgres` | PostgreSQL | ON CONFLICT DO NOTHING | `conflict_columns` |
| `upsert_mssql` | MSSQL | MERGE | `conflict_columns` |
| `upsert_impala` | Kudu | UPSERT INTO | ไม่ต้อง (ใช้ PK ของ table) |
| `upsert_parquet_impala` | Parquet | Staging + INSERT OVERWRITE | `key_columns` |
| `insert_incremental_parquet_impala` | Parquet | Staging + LEFT ANTI JOIN | `key_columns` |
| `upsert_iceberg` | Iceberg | MERGE INTO | `key_columns` |
| `insert_incremental_iceberg` | Iceberg | MERGE INTO (no update) | `key_columns` |

---

## เลือก Function ไหนดี?

| Use Case | Function |
|---|---|
| อัปเดตข้อมูลล่าสุด + เพิ่มแถวใหม่ (PostgreSQL) | `upsert_postgres` |
| Append-only logs/events — ข้ามซ้ำ (PostgreSQL) | `insert_do_nothing_postgres` |
| อัปเดต + เพิ่มใหม่ (MSSQL) | `upsert_mssql` |
| Real-time data ใส่ Kudu | `upsert_impala` |
| Batch update Parquet table (small-medium) | `upsert_parquet_impala` |
| Append ลง Parquet — ข้ามซ้ำ | `insert_incremental_parquet_impala` |
| ACID upsert (Iceberg, CDP 7.1.3+) | `upsert_iceberg` |
| Append ลง Iceberg — ข้ามซ้ำ | `insert_incremental_iceberg` |
