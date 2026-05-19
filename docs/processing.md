# Processing Engines — Pandas vs Polars vs Dask

เลือก processing library ที่เหมาะสมตาม data volume และ workload

---

## Decision Matrix

```
Data Volume?
│
├─ < 1M rows ──────────────→ pandas (default)
│
├─ 1M – 10M rows
│   ├─ Transform-heavy? ──→ polars (3-10x faster)
│   └─ Simple ETL? ───────→ pandas (ยัง OK)
│
├─ 10M – 100M rows
│   ├─ Single machine? ───→ polars (lazy) หรือ dask
│   └─ Multi-file? ───────→ dask
│
└─ > 100M rows ────────────→ Spark (cluster)
```

---

## Comparison Table

| Volume | Library | RAM Usage | Speed | Use Case |
|---|---|---|---|---|
| < 1M rows | **pandas** ✅ | 1x (baseline) | 1x | ETL ทั่วไป (ใช้ใน template) |
| 1M–10M rows | **polars** | 0.3-0.5x | 3-10x | Transform-heavy, single machine |
| 10M–100M rows | **dask** | streaming | 2-5x | Out-of-core, cluster |
| > 100M rows | **Spark** | distributed | N/A | Cluster compute |

---

## Pandas (default ใน template)

```python
import pandas as pd

df = pd.read_sql("SELECT * FROM source", conn)  # < 1M rows
df["amount_thb"] = df["amount"] * 35.0
upsert_postgres(conn, df, "target", conflict_columns=["id"])
```

**ข้อดี:** ecosystem ใหญ่, ทุกคนรู้จัก, compatible ทุก library
**ข้อจำกัด:** RAM = 2-5x ของ data size, single-threaded

### Best Practices

```python
# 1. อ่าน column ที่ต้องใช้เท่านั้น
df = pd.read_sql("SELECT id, name, amount FROM source", conn)

# 2. ใช้ dtype ที่เหมาะ — ลด RAM 50%+
df["category"] = df["category"].astype("category")
df["id"] = pd.to_numeric(df["id"], downcast="integer")

# 3. Chunk read สำหรับ data ใหญ่
for chunk in pd.read_sql("SELECT * FROM big_table", conn, chunksize=50000):
    process(chunk)
```

---

## Polars (เร็วกว่า pandas 3-10x)

```python
import polars as pl

# อ่านไฟล์ — lazy evaluation
df = (
    pl.scan_parquet("data/*.parquet")
    .filter(pl.col("date") >= "2025-01-01")
    .group_by("category")
    .agg(pl.col("amount").sum())
    .collect()  # execute ตรงนี้
)

# แปลงเป็น pandas เพื่อใช้กับ upsert functions
pdf = df.to_pandas()
upsert_postgres(conn, pdf, "summary", conflict_columns=["category"])
```

**ข้อดี:**
- เร็วกว่า pandas 3-10x (Rust engine, multi-threaded)
- RAM ใช้น้อยกว่า 50-70% (Apache Arrow memory)
- Lazy evaluation — ไม่ load จนกว่าจะ `.collect()`
- ไม่มี GIL → ใช้ทุก core

**เมื่อไหร่ใช้:**
- Data > 1M rows
- Transform-heavy (group by, join, window functions)
- RAM จำกัด

**Install:**
```bash
uv add polars
```

### Polars — Common Patterns

```python
# Read SQL → Polars (ผ่าน connectorx — เร็วกว่า pandas.read_sql 2-3x)
df = pl.read_database_uri("SELECT * FROM table", uri)

# Window functions
df = df.with_columns(
    pl.col("amount").sum().over("category").alias("category_total"),
    pl.col("amount").rank().over("category").alias("rank_in_category"),
)

# Multiple aggregations
summary = df.group_by("department").agg(
    pl.col("salary").mean().alias("avg_salary"),
    pl.col("salary").max().alias("max_salary"),
    pl.len().alias("headcount"),
)

# Join
result = orders.join(customers, on="customer_id", how="left")
```

---

## Dask (Out-of-core / Cluster)

```python
import dask.dataframe as dd

# อ่าน file ใหญ่กว่า RAM ได้
ddf = dd.read_parquet("hdfs:///data/events/year=2025/**/*.parquet")
result = ddf.groupby("user_id").amount.sum().compute()

# หรือ process chunk-by-chunk
for chunk in ddf.to_delayed():
    pdf = chunk.compute()  # pandas DataFrame per partition
    upsert_postgres(conn, pdf, "aggregates", conflict_columns=["user_id"])
```

**ข้อดี:**
- Process data ใหญ่กว่า RAM (streaming/partitioned)
- Scale จาก laptop → cluster (Dask distributed)
- API เหมือน pandas

**เมื่อไหร่ใช้:**
- Data > 10M rows หรือ > RAM
- ต้องอ่านจาก HDFS/S3 หลายไฟล์
- ต้องการ parallel processing across files

**Install:**
```bash
uv add dask[dataframe]
```

### Dask — Distributed Cluster

```python
from dask.distributed import Client

# Local cluster (ใช้ทุก core)
client = Client()

# Remote cluster
client = Client("tcp://scheduler:8786")

# Process
ddf = dd.read_parquet("s3://bucket/data/**/*.parquet")
result = ddf.groupby("region").agg({"amount": "sum"}).compute()
```

---

## Spark (> 100M rows / Cluster)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("etl-job") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

df = spark.read.parquet("hdfs:///data/big_table")
result = df.groupBy("category").agg({"amount": "sum"})
result.write.mode("overwrite").parquet("hdfs:///output/summary")
```

**เมื่อไหร่ใช้:**
- Data > 100M rows
- มี Hadoop/Spark cluster อยู่แล้ว
- ต้อง distributed JOIN ข้าม table ใหญ่ๆ

---

## Template Integration

ใน template ใช้ **pandas** เป็น default เพราะ:
1. ทุก connector return/accept pandas DataFrame
2. upsert functions รับ `pd.DataFrame`
3. Schema generators อ่าน dtype จาก pandas

**ถ้าจะใช้ Polars/Dask:**
```python
# job/my_heavy_job.py
import polars as pl
from utils.upsert import upsert_postgres

class MyHeavyJob(BaseJob):
    name = "heavy_transform"

    def run(self, settings: Settings):
        # Process ด้วย Polars (เร็ว)
        df = (
            pl.scan_parquet("/data/big_file.parquet")
            .filter(...)
            .group_by(...)
            .agg(...)
            .collect()
        )

        # แปลงเป็น pandas ตอน insert (compatible กับ upsert functions)
        with PostgresConnector(...) as pg:
            upsert_postgres(pg.conn, df.to_pandas(), "target", ["id"])
```

---

## Benchmark Reference (100K rows, 20 columns)

| Operation | pandas | polars | Speedup |
|---|---|---|---|
| Read CSV (500MB) | 8s | 1.2s | 6.7x |
| GroupBy + Agg | 2.5s | 0.3s | 8.3x |
| Join (2 tables) | 3s | 0.5s | 6x |
| Filter + Transform | 1.5s | 0.2s | 7.5x |
| Memory usage | 1.2 GB | 0.4 GB | 3x less |

---

## Migration Guide: pandas → polars

| pandas | polars | หมายเหตุ |
|---|---|---|
| `df["col"]` | `df["col"]` หรือ `pl.col("col")` | polars ใช้ expression API |
| `df.groupby("a").sum()` | `df.group_by("a").agg(pl.all().sum())` | explicit agg |
| `df.merge(other, on="id")` | `df.join(other, on="id")` | — |
| `df.apply(fn)` | `df.with_columns(pl.col("x").map_elements(fn))` | หลีกเลี่ยง — ช้า |
| `df.sort_values("a")` | `df.sort("a")` | — |
| `df.fillna(0)` | `df.fill_null(0)` | — |
| `pd.read_csv(f)` | `pl.read_csv(f)` / `pl.scan_csv(f)` | scan = lazy |

**กฎทอง:** ถ้า polars มี built-in expression → ใช้เสมอ (เร็วกว่า `map_elements` 100x+)

---

## Summary

| Library | Best For | RAM | Speed vs pandas | Learning Curve |
|---|---|---|---|---|
| **pandas** | < 1M rows, ทุก ETL | 2-5x data | 1x (baseline) | ต่ำ |
| **polars** | 1-10M rows, transforms | 0.3-0.5x data | 3-10x | กลาง |
| **dask** | > 10M, multi-file, cluster | streaming | 2-5x | กลาง |
| **Spark** | > 100M, distributed | distributed | N/A | สูง |
