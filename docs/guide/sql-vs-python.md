# SQL vs Python — เมื่อไหร่ใช้อะไร

หลักการเลือกว่า logic ควรอยู่ใน SQL (DB engine) หรือ Python (application)

---

## กฎง่ายๆ

| ถ้า... | ใช้ |
|---|---|
| Data อยู่ใน DB อยู่แล้ว + ผลลัพธ์ก็ลง DB | **SQL** |
| ต้อง cross-system (DB A → DB B) | **Python** (extract) + SQL (transform ใน target) |
| Logic ซับซ้อน (ML, API, regex, custom) | **Python** |
| Aggregation / JOIN / Window function | **SQL** (เร็วกว่า 10-100x) |
| Data อยู่ใน file (CSV, Parquet, API) | **Python** |

---

## SQL ดีกว่า Python ตรงไหน

### 1. Aggregation & GROUP BY

```sql
-- ✅ SQL — DB engine parallelize, ใช้ index, ไม่ต้อง load data ออกมา
SELECT
    department,
    COUNT(*) as headcount,
    AVG(salary) as avg_salary,
    MAX(salary) as max_salary
FROM employees
GROUP BY department;
```

```python
# ❌ Python — ต้อง load ทุก row เข้า RAM ก่อน
df = pd.read_sql("SELECT * FROM employees", conn)  # load 1M rows → RAM
result = df.groupby("department").agg({"salary": ["count", "mean", "max"]})
```

**ต่างกัน:** 1M rows → SQL ~0.5s, Python ~3s + 500MB RAM

### 2. JOIN

```sql
-- ✅ SQL — hash join / merge join ใน DB engine (optimized)
SELECT o.*, c.name as customer_name, p.name as product_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
WHERE o.order_date >= '2025-01-01';
```

```python
# ❌ Python — load 3 tables เข้า RAM แล้ว merge
orders = pd.read_sql("SELECT * FROM orders", conn)
customers = pd.read_sql("SELECT * FROM customers", conn)
products = pd.read_sql("SELECT * FROM products", conn)
result = orders.merge(customers, on="customer_id").merge(products, on="product_id")
# RAM: 3 tables + merged result = 4x
```

**กฎ:** ถ้า JOIN ได้ใน DB เดียวกัน → ทำใน SQL เสมอ

### 3. Window Functions

```sql
-- ✅ SQL — running total, rank, lag/lead
SELECT
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date) as running_total,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sale_date DESC) as rn,
    LAG(amount) OVER (PARTITION BY customer_id ORDER BY sale_date) as prev_amount
FROM sales;
```

```python
# ❌ Python — ทำได้แต่ช้ากว่า + ใช้ RAM เยอะ
df["running_total"] = df["amount"].cumsum()
df["rn"] = df.groupby("customer_id")["sale_date"].rank(ascending=False)
df["prev_amount"] = df.groupby("customer_id")["amount"].shift(1)
```

### 4. Filtering (WHERE)

```sql
-- ✅ SQL — filter ที่ source (ดึงเฉพาะที่ต้องการ)
SELECT * FROM events
WHERE event_date >= '2025-01-01'
  AND event_type = 'purchase'
  AND amount > 1000;
-- Return: 50K rows (จาก 10M)
```

```python
# ❌ Python — load ทุก row แล้วมา filter
df = pd.read_sql("SELECT * FROM events", conn)  # 10M rows → RAM (5 GB!)
df = df[(df["event_date"] >= "2025-01-01") &
        (df["event_type"] == "purchase") &
        (df["amount"] > 1000)]
# ใช้ 5 GB RAM เพื่อได้ 50K rows
```

**กฎ:** Filter ที่ source เสมอ — ไม่ `SELECT *` แล้วมา filter ใน Python

### 5. UPSERT / MERGE

```sql
-- ✅ SQL — atomic, ใช้ index, ไม่ต้อง round-trip
INSERT INTO target (id, name, amount)
VALUES (1, 'a', 100), (2, 'b', 200)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, amount = EXCLUDED.amount;
```

```python
# ✅ Python — wrapper ที่ generate SQL ข้างบน (template ทำแบบนี้)
upsert_postgres(conn, df, "target", conflict_columns=["id"])
# ภายในสร้าง INSERT ... ON CONFLICT ... DO UPDATE
```

### 6. DELETE with condition

```sql
-- ✅ SQL — ลบตรงๆ ไม่ต้อง load data
DELETE FROM events WHERE event_date < '2024-01-01';
DELETE FROM staging WHERE processed = TRUE;
```

```python
# ❌ Python — load มาแล้ว filter แล้ว delete ทีละ row??
# ไม่มีเหตุผลจะทำใน Python
```

---

## Python ดีกว่า SQL ตรงไหน

### 1. Cross-system Transfer

```python
# ✅ Python — MSSQL → PostgreSQL (ต่าง engine)
df = pd.read_sql("SELECT * FROM erp.sales", mssql_conn)
upsert_postgres(pg_conn, df, "dwh.fact_sales", conflict_columns=["id"])
```

```sql
-- ❌ SQL — ทำไม่ได้ (คนละ DB engine)
INSERT INTO pg_target SELECT * FROM mssql_source;
```

### 2. Complex String Processing

```python
# ✅ Python — regex, NLP, custom parsing
import re

df["phone_clean"] = df["phone"].apply(lambda x: re.sub(r"[^\d]", "", str(x)))
df["name_parts"] = df["full_name"].str.split(r"\s+")
df["email_domain"] = df["email"].str.extract(r"@(.+)$")

# Thai text processing
df["province"] = df["address"].apply(extract_thai_province)
```

```sql
-- ❌ SQL — regex support จำกัด, ช้า, อ่านยาก
SELECT REGEXP_REPLACE(phone, '[^\d]', '', 'g') as phone_clean FROM t;
-- ไม่มี NLP, ไม่มี custom function
```

### 3. API Calls / External Services

```python
# ✅ Python — call API ระหว่าง ETL
import requests

def enrich_with_api(df):
    for idx, row in df.iterrows():
        resp = requests.get(f"https://api.service.com/lookup/{row['code']}")
        df.loc[idx, "extra_data"] = resp.json().get("value")
    return df
```

### 4. ML / Statistical Operations

```python
# ✅ Python — ML libraries
from sklearn.preprocessing import StandardScaler

scaler = StandardScaler()
df[["amount_scaled"]] = scaler.fit_transform(df[["amount"]])

# Outlier detection
df["is_outlier"] = (df["amount"] > df["amount"].mean() + 3 * df["amount"].std())
```

### 5. File Processing

```python
# ✅ Python — CSV, Excel, Parquet, JSON, XML
df = pd.read_csv("data.csv")
df = pd.read_excel("report.xlsx", sheet_name="Sheet1")
df = pd.read_parquet("events/*.parquet")

# Custom file format
with open("legacy_fixed_width.txt") as f:
    records = [parse_fixed_width(line) for line in f]
```

### 6. Data Validation & Cleansing

```python
# ✅ Python — complex business rules
def validate_row(row):
    errors = []
    if row["amount"] < 0:
        errors.append("negative_amount")
    if row["email"] and not is_valid_email(row["email"]):
        errors.append("invalid_email")
    if row["start_date"] > row["end_date"]:
        errors.append("date_range_invalid")
    return errors

df["validation_errors"] = df.apply(validate_row, axis=1)
valid_df = df[df["validation_errors"].apply(len) == 0]
```

### 7. Conditional Logic (Complex)

```python
# ✅ Python — complex branching ที่ SQL ทำได้แต่อ่านยากมาก
def classify_customer(row):
    if row["total_spend"] > 100000 and row["frequency"] > 10:
        return "VIP"
    elif row["last_purchase_days"] > 365:
        return "Churned"
    elif row["total_spend"] > 50000:
        return "Premium"
    else:
        return "Regular"

df["segment"] = df.apply(classify_customer, axis=1)
```

```sql
-- ❌ SQL — ทำได้แต่ maintain ยาก ถ้าซับซ้อนมากขึ้น
SELECT *,
    CASE
        WHEN total_spend > 100000 AND frequency > 10 THEN 'VIP'
        WHEN last_purchase_days > 365 THEN 'Churned'
        WHEN total_spend > 50000 THEN 'Premium'
        ELSE 'Regular'
    END as segment
FROM customers;
```

> **หมายเหตุ:** CASE WHEN ง่ายๆ 2-3 conditions → SQL ก็ OK ไม่ต้องมา Python

---

## Hybrid Pattern (แนะนำ)

### ELT: Extract (Python) → Load Raw (Python) → Transform (SQL)

```python
# Step 1: Extract from source (Python — cross-system)
df = pd.read_sql("SELECT * FROM erp.sales", mssql_conn)

# Step 2: Load raw to target (Python — upsert)
upsert_postgres(pg_conn, df, "raw.sales", conflict_columns=["id"])

# Step 3: Transform in target DB (SQL — fast, uses indexes)
pg_conn.execute("""
    INSERT INTO dwh.fact_sales (date_key, product_key, customer_key, amount)
    SELECT
        d.date_key,
        p.product_key,
        c.customer_key,
        s.amount * fx.rate as amount_thb
    FROM raw.sales s
    JOIN dwh.dim_date d ON s.sale_date = d.full_date
    JOIN dwh.dim_product p ON s.product_id = p.source_id
    JOIN dwh.dim_customer c ON s.customer_id = c.source_id
    JOIN ref.fx_rate fx ON s.currency = fx.currency AND s.sale_date = fx.rate_date
    ON CONFLICT (date_key, product_key, customer_key)
    DO UPDATE SET amount = EXCLUDED.amount
""")
```

### ETL: Extract (Python) → Transform (Python) → Load (Python→SQL)

```python
# เหมาะเมื่อ: transform ซับซ้อน, ต้อง call API, cross-system
df = pd.read_sql("SELECT * FROM source", mssql_conn)

# Python transform (complex logic ที่ SQL ทำไม่ได้/ยาก)
df["segment"] = df.apply(classify_customer, axis=1)
df["geo"] = df["address"].apply(geocode_address)  # API call
df = df[validate_business_rules(df)]  # custom validation

# Load (Python generate SQL)
upsert_postgres(pg_conn, df, "target", conflict_columns=["id"])
```

---

## Performance Comparison

### 1M rows, 20 columns

| Operation | SQL (in-DB) | Python (pandas) | Winner |
|---|---|---|---|
| SELECT with WHERE | 0.1s | 3s (load all) + 0.1s (filter) | SQL 30x |
| GROUP BY + AGG | 0.5s | 3s (load) + 0.8s (groupby) | SQL 8x |
| JOIN 2 tables | 0.3s | 6s (load both) + 2s (merge) | SQL 27x |
| Window function | 0.8s | 3s (load) + 1.5s (window) | SQL 6x |
| INSERT 100K rows | 1s (COPY) | 1s (execute_values) | Tie |
| Complex regex | 5s | 3s (load) + 2s (apply) | Python 1x |
| ML scoring | N/A | 3s (load) + 0.5s (predict) | Python only |

### Key Insight

> **ถ้า data อยู่ใน DB แล้ว + output ก็ลง DB → ใช้ SQL**
> ไม่มีเหตุผลที่จะ load ออกมา → process ใน Python → ส่งกลับ DB

---

## SQL Pushdown — ลด Data Transfer

```python
# ❌ BAD — load ทุกอย่างมา Python
df = pd.read_sql("SELECT * FROM orders", conn)  # 5M rows, 2 GB
df = df[df["status"] == "completed"]  # filter ใน Python
df = df.groupby("product_id")["amount"].sum()  # groupby ใน Python

# ✅ GOOD — ให้ DB ทำ heavy lifting
df = pd.read_sql("""
    SELECT product_id, SUM(amount) as total
    FROM orders
    WHERE status = 'completed'
    GROUP BY product_id
""", conn)  # 1K rows, 50 KB
```

**Pushdown Checklist:**
- [ ] WHERE clause → filter ที่ source
- [ ] SELECT เฉพาะ column ที่ต้องใช้ (ไม่ `SELECT *`)
- [ ] GROUP BY ใน SQL ถ้า aggregate ก่อน load
- [ ] JOIN ใน SQL ถ้า tables อยู่ DB เดียวกัน
- [ ] LIMIT ถ้าต้องการ sample

---

## Decision Flowchart

```
ต้องทำอะไร?
│
├─ Filter / Aggregate / Join
│   ├─ Data อยู่ DB เดียวกัน? ──→ SQL
│   └─ Data ต่าง DB? ──→ Python extract → SQL ใน target
│
├─ Complex Transform
│   ├─ CASE WHEN 2-3 conditions? ──→ SQL
│   ├─ Business logic 10+ rules? ──→ Python
│   ├─ Regex / NLP / ML? ──→ Python
│   └─ API enrichment? ──→ Python
│
├─ Data Movement
│   ├─ Same DB (table → table)? ──→ SQL (INSERT...SELECT)
│   └─ Cross DB? ──→ Python (extract → load)
│
└─ File Processing
    └─ Always ──→ Python
```

---

## Template Guideline

ใน ETL template นี้:

| Layer | ใช้ | ตัวอย่าง |
|---|---|---|
| **Extract** | Python + SQL query | `pd.read_sql("SELECT ... WHERE ...", conn)` |
| **Transform (simple)** | SQL ใน query | JOIN, GROUP BY, CASE WHEN |
| **Transform (complex)** | Python | validate, enrich, ML, API |
| **Load** | Python → SQL | `upsert_postgres()` → generates INSERT...ON CONFLICT |
| **Post-load transform** | SQL | Stored procedure, materialized view |

### ตัวอย่าง Job ที่ดี

```python
class DailySalesJob(BaseJob):
    name = "daily_sales"

    def run(self, settings: Settings):
        with MSSQLConnector(...) as src, PostgresConnector(...) as tgt:
            # Extract — ให้ SQL ทำ JOIN + filter + aggregate ที่ source
            df = pd.read_sql("""
                SELECT
                    s.sale_date,
                    s.product_id,
                    p.category,
                    SUM(s.amount) as total_amount,
                    COUNT(*) as txn_count
                FROM sales s
                JOIN products p ON s.product_id = p.id
                WHERE s.sale_date = CAST(GETDATE()-1 AS DATE)
                GROUP BY s.sale_date, s.product_id, p.category
            """, src.conn)

            # Transform — เฉพาะ logic ที่ SQL ทำไม่ได้
            df["amount_thb"] = df["total_amount"] * get_fx_rate("USD", "THB")
            df["segment"] = df["category"].map(CATEGORY_SEGMENT_MAP)

            # Load
            upsert_postgres(tgt.conn, df, "dwh.daily_sales",
                          conflict_columns=["sale_date", "product_id"])
```

---

## Summary

| ใช้ SQL เมื่อ | ใช้ Python เมื่อ |
|---|---|
| Data อยู่ใน DB อยู่แล้ว | Cross-system transfer |
| Filter (WHERE) | Complex validation |
| Aggregate (GROUP BY) | API calls / external service |
| JOIN (same DB) | ML / statistical ops |
| Window functions | Complex string/regex |
| Simple CASE WHEN | File processing |
| INSERT...SELECT (same DB) | Custom business logic (10+ rules) |
| DELETE / UPDATE with condition | Data ที่มาจาก file/API |

**Golden Rule:** อย่า load data ออกจาก DB มา Python ถ้าไม่จำเป็น — ให้ DB ทำงานแทน
