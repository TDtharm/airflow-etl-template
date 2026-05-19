# Data Pipeline Patterns

รูปแบบ pipeline ที่พบบ่อยใน production ETL พร้อมตัวอย่างที่ใช้กับ template

---

## Full Load vs Incremental

### Full Load (ทั้งหมด)

```python
# ดึงข้อมูลทั้ง table แล้ว replace
df = pd.read_sql("SELECT * FROM source_table", source_conn)
upsert_postgres(target_conn, df, "target_table", conflict_columns=["id"])
```

**เมื่อไหร่ใช้:**
- Table เล็ก (< 100K rows)
- Source ไม่มี updated_at / change tracking
- ต้องการ simplicity — ไม่ต้อง track state

**ข้อดี:** simple, ไม่ต้อง track watermark, self-healing (ถ้าพลาด run ครั้งก่อนก็ได้ข้อมูลครบ)
**ข้อเสีย:** ช้า, load source หนัก, network bandwidth สูง

### Incremental Load (เฉพาะที่เปลี่ยน)

```python
# ดึงเฉพาะ row ที่เปลี่ยนตั้งแต่ last run
last_watermark = get_watermark("my_job")  # เก็บใน metadata table
df = pd.read_sql(
    f"SELECT * FROM source WHERE updated_at > '{last_watermark}'",
    source_conn,
)
upsert_postgres(target_conn, df, "target_table", conflict_columns=["id"])
set_watermark("my_job", df["updated_at"].max())
```

**เมื่อไหร่ใช้:**
- Table ใหญ่ (> 100K rows)
- Source มี `updated_at` หรือ `modified_date`
- ต้องการลด load time

**Watermark Storage:**
```python
# utils/watermark.py
def get_watermark(job_name: str, conn) -> datetime:
    row = conn.execute(
        "SELECT watermark FROM etl_metadata WHERE job = %s", (job_name,)
    ).fetchone()
    return row[0] if row else datetime(2000, 1, 1)

def set_watermark(job_name: str, value: datetime, conn):
    conn.execute("""
        INSERT INTO etl_metadata (job, watermark, updated_at)
        VALUES (%s, %s, NOW())
        ON CONFLICT (job) DO UPDATE SET watermark = %s, updated_at = NOW()
    """, (job_name, value, value))
    conn.commit()
```

---

## CDC (Change Data Capture)

### Timestamp-based CDC (ใช้บ่อยสุด)

```python
# prod_hourly_incremental pattern
df = pd.read_sql(f"""
    SELECT * FROM source
    WHERE updated_at BETWEEN '{start}' AND '{end}'
""", conn)
upsert_postgres(target_conn, df, "target", conflict_columns=["id"])
```

**ข้อจำกัด:** ไม่จับ DELETE ได้ (ต้อง soft-delete ที่ source)

### Log-based CDC (Debezium / WAL)

```python
# อ่าน CDC events จาก NATS/Kafka
from utils.connectors import NATSConnector

with NATSConnector(...) as nats:
    messages = nats.subscribe("cdc.source_table")
    for msg in messages:
        event = json.loads(msg.data)
        if event["op"] == "c":  # CREATE
            insert_row(event["after"])
        elif event["op"] == "u":  # UPDATE
            upsert_row(event["after"])
        elif event["op"] == "d":  # DELETE
            delete_row(event["before"]["id"])
```

**ข้อดี:** จับทุก operation (INSERT/UPDATE/DELETE), near real-time
**ข้อเสีย:** ต้อง setup Debezium/connector, complex infrastructure

### Query-based CDC (compare snapshots)

```python
# เปรียบเทียบ hash ของ row เพื่อหา changes
source_df = pd.read_sql("SELECT id, md5(row_to_json(t)::text) as hash FROM source t", conn)
target_df = pd.read_sql("SELECT id, row_hash as hash FROM target", conn)

# หา rows ที่เปลี่ยน
changed_ids = source_df.merge(target_df, on="id", how="left", suffixes=("_src", "_tgt"))
changed_ids = changed_ids[changed_ids["hash_src"] != changed_ids["hash_tgt"]]["id"]

# ดึง full rows ที่เปลี่ยนมา upsert
df = pd.read_sql(f"SELECT * FROM source WHERE id IN ({id_list})", conn)
upsert_postgres(target_conn, df, "target", conflict_columns=["id"])
```

---

## SCD (Slowly Changing Dimensions)

### Type 1 — Overwrite

```python
# เขียนทับค่าเก่า (ไม่เก็บ history)
upsert_postgres(conn, df, "dim_customer", conflict_columns=["customer_id"])
# ON CONFLICT (customer_id) DO UPDATE SET name = EXCLUDED.name, ...
```

**เมื่อไหร่ใช้:** ไม่สนใจ history, ต้องการ latest value เท่านั้น

### Type 2 — Add Row (เก็บ history)

```python
from datetime import datetime

def scd_type2_upsert(conn, df, table, key_cols, track_cols):
    """
    SCD Type 2: expire old row + insert new row
    """
    now = datetime.now()

    for _, row in df.iterrows():
        key_filter = " AND ".join(f"{k} = %s" for k in key_cols)
        key_values = [row[k] for k in key_cols]

        # 1. Check if any tracked column changed
        existing = pd.read_sql(f"""
            SELECT * FROM {table}
            WHERE {key_filter} AND is_current = TRUE
        """, conn, params=key_values)

        if existing.empty:
            # New row — insert
            row["effective_from"] = now
            row["effective_to"] = None
            row["is_current"] = True
            insert_row(conn, table, row)
        else:
            # Check if changed
            old = existing.iloc[0]
            changed = any(row[c] != old[c] for c in track_cols)
            if changed:
                # Expire old
                conn.execute(f"""
                    UPDATE {table} SET is_current = FALSE, effective_to = %s
                    WHERE {key_filter} AND is_current = TRUE
                """, [now] + key_values)
                # Insert new version
                row["effective_from"] = now
                row["effective_to"] = None
                row["is_current"] = True
                insert_row(conn, table, row)

    conn.commit()
```

**Schema ที่ต้องมี:**
```sql
CREATE TABLE dim_customer (
    id              SERIAL PRIMARY KEY,
    customer_id     INTEGER NOT NULL,  -- business key
    name            TEXT,
    email           TEXT,
    effective_from  TIMESTAMP NOT NULL,
    effective_to    TIMESTAMP,
    is_current      BOOLEAN DEFAULT TRUE
);
CREATE INDEX idx_customer_current ON dim_customer (customer_id, is_current);
```

### Type 3 — Add Column (เก็บ previous value)

```sql
-- เก็บ column เดิม + column ใหม่
ALTER TABLE dim_customer ADD COLUMN prev_email TEXT;

-- Update
UPDATE dim_customer
SET prev_email = email, email = 'new@example.com'
WHERE customer_id = 123;
```

**เมื่อไหร่ใช้:** ต้องการเก็บ previous value แค่ 1 version

---

## Idempotency

หลักการ: **run ETL กี่ครั้งก็ได้ผลลัพธ์เหมือนกัน**

### Pattern 1: UPSERT (default ใน template)

```python
# ON CONFLICT DO UPDATE — run ซ้ำได้เสมอ
upsert_postgres(conn, df, "target", conflict_columns=["id"])
```

### Pattern 2: DELETE + INSERT (partition-level)

```python
# ลบ partition ที่จะ load แล้ว insert ใหม่
def idempotent_load(conn, df, table, partition_col, partition_value):
    conn.execute(f"DELETE FROM {table} WHERE {partition_col} = %s", (partition_value,))
    insert_batch(conn, df, table)
    conn.commit()

# ใช้
idempotent_load(conn, df, "sales", "sale_date", "2025-01-15")
```

### Pattern 3: INSERT OVERWRITE (Hive/Iceberg)

```sql
-- Impala/Iceberg — overwrite partition
INSERT OVERWRITE TABLE sales PARTITION (dt = '2025-01-15')
SELECT * FROM staging_sales WHERE dt = '2025-01-15';
```

### ทำไม Idempotency สำคัญ:
- Airflow retry task → ไม่ duplicate data
- Manual re-run → ผลลัพธ์ consistent
- Backfill → safe ทุก run

---

## ELT vs ETL

### ETL (Extract → Transform → Load)

```python
# Transform ใน Python ก่อน load
df = pd.read_sql("SELECT * FROM source", source_conn)
df["amount_thb"] = df["amount_usd"] * 35.0
df["category"] = df["raw_category"].map(category_mapping)
upsert_postgres(target_conn, df, "target", conflict_columns=["id"])
```

**เมื่อไหร่ใช้:**
- Transform logic ซับซ้อน (ML, API calls, custom logic)
- Source/Target ต่าง DB engine
- ต้องการ pandas/polars processing

### ELT (Extract → Load → Transform)

```python
# Load raw ก่อน, transform ใน DB (SQL)
df = pd.read_sql("SELECT * FROM source", source_conn)
upsert_postgres(target_conn, df, "raw_source", conflict_columns=["id"])

# Transform ใน DB (faster for large data)
target_conn.execute("""
    INSERT INTO mart_sales
    SELECT
        date_trunc('month', sale_date) as month,
        category,
        SUM(amount * 35.0) as total_thb
    FROM raw_source
    GROUP BY 1, 2
    ON CONFLICT (month, category) DO UPDATE SET total_thb = EXCLUDED.total_thb
""")
```

**เมื่อไหร่ใช้:**
- Transform เป็น SQL ได้ (aggregation, joins)
- Target DB แรง (PG, Spark)
- Data volume ใหญ่ — ให้ DB engine parallelize

### Hybrid (template default)

```python
# Extract + light transform ใน Python
df = pd.read_sql("SELECT * FROM source", source_conn)
df = df.rename(columns={"old_col": "new_col"})  # light transform
upsert_postgres(target_conn, df, "staging", conflict_columns=["id"])

# Heavy transform ใน DB
target_conn.execute("CALL sp_build_mart()")  # stored procedure
```

---

## Star Schema Pattern

ดูตัวอย่างเต็มใน `dags/prod_daily_etl.py`

```
[Extract Sources] → [Load Dimensions] → [Load Facts] → [DQ Check]
     (parallel)         (parallel)          (sequential)
```

```python
# Dimension tables (parallel)
extract_customers >> load_dim_customer
extract_products  >> load_dim_product
extract_stores    >> load_dim_store

# Fact tables (depend on dimensions)
[load_dim_customer, load_dim_product, load_dim_store] >> load_fact_sales

# Data quality
load_fact_sales >> dq_check
```

---

## Backfill Pattern

```python
# DAG ที่รองรับ backfill — ใช้ execution_date ไม่ใช่ NOW()
def extract(**context):
    exec_date = context["execution_date"]
    start = exec_date.strftime("%Y-%m-%d")
    end = (exec_date + timedelta(days=1)).strftime("%Y-%m-%d")

    df = pd.read_sql(f"""
        SELECT * FROM source
        WHERE created_at >= '{start}' AND created_at < '{end}'
    """, conn)
    return df

# Backfill command:
# airflow dags backfill my_dag --start-date 2025-01-01 --end-date 2025-01-31
```

**กฎ Backfill:**
1. ใช้ `execution_date` (logical date) เสมอ — ไม่ใช้ `datetime.now()`
2. ETL ต้อง idempotent (UPSERT หรือ DELETE+INSERT per partition)
3. ไม่ depend on previous run (self-contained per interval)

---

## Summary

| Pattern | เมื่อไหร่ | Complexity | Template Support |
|---|---|---|---|
| **Full Load** | Table เล็ก, ไม่มี watermark | ต่ำ | ✅ upsert functions |
| **Incremental** | Table ใหญ่, มี updated_at | กลาง | ✅ watermark + upsert |
| **CDC (timestamp)** | Near real-time, ไม่ต้อง DELETE | กลาง | ✅ hourly DAG example |
| **CDC (log-based)** | Real-time, ต้องจับ DELETE | สูง | NATS connector |
| **SCD Type 1** | ไม่เก็บ history | ต่ำ | ✅ upsert = SCD1 |
| **SCD Type 2** | เก็บ history ทุก version | สูง | custom function |
| **ELT** | Transform เป็น SQL, data ใหญ่ | กลาง | ✅ raw load + SQL |
| **Backfill** | Re-process historical data | กลาง | ✅ execution_date |
