# Scaling — DB Insert

เลือก library ที่เหมาะสมตาม data volume สำหรับ insert/upsert แต่ละ DB

---

## PostgreSQL

| Volume | Library | Speed (100K rows) | วิธี |
|---|---|---|---|
| < 100K | `psycopg2` + `execute_values` ✅ | ~3s | VALUES batch (ใช้ใน template) |
| 100K–1M | `psycopg2` + `COPY FROM` | ~1s | Binary stream protocol |
| > 1M | `asyncpg` + `copy_records_to_table` | ~0.5s | Async binary COPY |
| > 10M | `pg_bulkload` / external file | ~0.3s | Bypass WAL |

### COPY FROM (เร็วกว่า execute_values 3-5x)

```python
import io

buffer = io.StringIO()
df.to_csv(buffer, index=False, header=False)
buffer.seek(0)

cur.copy_expert(
    f"COPY {schema}.{table} ({col_list}) FROM STDIN WITH CSV",
    buffer,
)
conn.commit()
```

**ทำไมเร็วกว่า:**
- `execute_values` → parse SQL statement per batch
- `COPY` → binary stream protocol, ไม่ parse SQL, ไม่มี round-trip per batch

### asyncpg (Async + Binary COPY)

```python
import asyncpg
import asyncio

async def bulk_insert(df, table):
    conn = await asyncpg.connect(dsn)
    records = list(df.itertuples(index=False, name=None))
    await conn.copy_records_to_table(table, records=records)
    await conn.close()

asyncio.run(bulk_insert(df, "my_table"))
```

**เมื่อไหร่ใช้:** > 1M rows, ต้องการ max throughput, ไม่ต้อง upsert (INSERT only)

### Upsert + COPY (hybrid)

```python
# 1. COPY ลง temp table
cur.execute(f"CREATE TEMP TABLE _stg (LIKE {table})")
# ... COPY FROM ลง _stg ...

# 2. INSERT ... ON CONFLICT จาก temp table
cur.execute(f"""
    INSERT INTO {table} SELECT * FROM _stg
    ON CONFLICT ({conflict_cols}) DO UPDATE SET ...
""")
cur.execute("DROP TABLE _stg")
```

---

## MSSQL

| Volume | Library | Speed (100K rows) | วิธี |
|---|---|---|---|
| < 50K | `pymssql` + MERGE ✅ | ~60s | Per-row MERGE (ใช้ใน template) |
| 50K–500K | `pymssql` + `_mssql.bulk_copy` | ~5s | TDS bulk copy |
| > 500K | `bcpandas` (bcp CLI) | ~3s | Flat file → BCP |
| > 1M | `pyodbc` + fast_executemany | ~8s | ODBC batch |

### pymssql bulk_copy

```python
# ไม่ต้อง bcp binary — ใช้ TDS bulk copy protocol
conn._conn.bulk_copy("my_table", rows, col_count=len(columns))
```

**ข้อจำกัด:** INSERT only (ไม่ใช่ MERGE)

### pyodbc fast_executemany

```python
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd}"
)
cur = conn.cursor()
cur.fast_executemany = True  # ← key setting
cur.executemany(
    "INSERT INTO t (a, b, c) VALUES (?, ?, ?)",
    rows,
)
conn.commit()
```

**เมื่อไหร่ใช้:** ต้องการ ODBC compatibility, > 50K rows INSERT

### bcpandas (fastest)

```python
import bcpandas

# ต้อง install: apt install mssql-tools msodbcsql18
bcpandas.to_sql(df, "my_table", conn, if_exists="append", batch_size=50000)
```

**Dependencies:** `bcp` CLI binary + ODBC driver (system dependency)

### Upsert + Bulk (hybrid)

```python
# 1. Bulk insert → staging table
bcpandas.to_sql(df, "#staging", conn, if_exists="replace")

# 2. MERGE จาก staging
conn.execute("""
    MERGE target AS t
    USING #staging AS s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.name = s.name
    WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name);
""")
```

---

## Impala/Kudu

| Volume | Library | Speed (100K rows) | วิธี |
|---|---|---|---|
| < 50K | `impyla` + UPSERT INTO ✅ | ~30s | Per-row (ใช้ใน template) |
| 50K–500K | `impyla` + Parquet staging | ~5s | INSERT...SELECT from staging |
| > 500K | Spark + Kudu connector | ~3s | Direct Kudu write |
| > 1M | Kudu Java client (subprocess) | ~2s | Native tablet write |

### Parquet Staging (ใช้ใน template — `upsert_parquet_impala`)

```
1. CREATE TABLE _stg LIKE target STORED AS PARQUET
2. INSERT INTO _stg VALUES (...) per row
3. INSERT OVERWRITE target = _stg UNION ALL (target LEFT ANTI JOIN _stg)
4. DROP TABLE _stg
```

เร็วกว่า per-row **10-50x** — เพราะ step 3 เป็น single MapReduce job

### Spark + Kudu (> 500K rows)

```python
# PySpark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars", "/opt/kudu-spark.jar") \
    .getOrCreate()

df = spark.read.parquet("/data/input.parquet")
df.write \
    .format("kudu") \
    .option("kudu.master", "master:7051") \
    .option("kudu.table", "my_table") \
    .mode("append") \
    .save()
```

---

## Iceberg

| Volume | Library | Speed (100K rows) | วิธี |
|---|---|---|---|
| < 100K | `impyla` + MERGE INTO ✅ | ~10s | Staging → MERGE (ใช้ใน template) |
| > 100K | Spark + Iceberg connector | ~3s | Native Iceberg write |
| > 1M | Spark + Iceberg (partitioned) | ~5s | Partition-level MERGE |

### Spark + Iceberg (> 100K rows)

```python
df.writeTo("db.my_table") \
    .option("merge-schema", "true") \
    .overwritePartitions()

# หรือ MERGE
spark.sql("""
    MERGE INTO db.target t
    USING staging s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

## Batch Size Tuning

ค่า default ใน template เป็น conservative — ปรับได้ตาม network/memory:

| DB | Default | Min | Max | ปัจจัยที่ส่งผล |
|---|---|---|---|---|
| PostgreSQL | 5,000 | 1,000 | 50,000 | Network latency, `work_mem` |
| MSSQL | 500 | 100 | 2,000 | Lock escalation threshold |
| Kudu | 2,000 | 500 | 5,000 | Tablet flush, RPC overhead |

**วิธี tune:**
```python
# เพิ่ม batch size ถ้า network latency สูง (remote DB)
upsert_postgres(conn, df, "table", conflict_columns=["id"], batch_size=10000)

# ลด batch size ถ้า OOM หรือ lock timeout
upsert_mssql(conn, df, "table", conflict_columns=["id"], batch_size=200)
```

---

## Summary

| DB | Default (template) | Scale Option | When |
|---|---|---|---|
| PostgreSQL | execute_values (5K) | COPY FROM / asyncpg | > 100K rows |
| MSSQL | pymssql MERGE (500) | bulk_copy / bcpandas | > 50K rows |
| Kudu | impyla UPSERT (2K) | Parquet staging / Spark | > 50K rows |
| Iceberg | impyla MERGE INTO | Spark + Iceberg | > 100K rows |
