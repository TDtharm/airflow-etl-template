# Error Handling — Production ETL

กลยุทธ์จัดการ error สำหรับ production data pipeline

---

## Retry Strategy

### Airflow Retry (DAG-level)

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,  # 5m → 10m → 20m
    "max_retry_delay": timedelta(minutes=30),
    "on_retry_callback": on_retry_callback,  # แจ้ง GChat
}
```

**เมื่อไหร่ retry ได้:**
- Network timeout (DB connection lost)
- HTTP 5xx (API server error)
- Lock timeout (DB busy)
- Resource temporarily unavailable

**เมื่อไหร่ห้าม retry:**
- Data validation failed (bad data = bad data ไม่ว่า retry กี่ครั้ง)
- Permission denied (ต้อง fix config)
- Schema mismatch (ต้อง fix code)

### Application-level Retry (ใน code)

```python
import time
from functools import wraps

def retry(max_attempts=3, backoff_factor=2, exceptions=(Exception,)):
    """Decorator สำหรับ retry function"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts:
                        raise
                    wait = backoff_factor ** attempt
                    logger.warning(f"Attempt {attempt} failed: {e}. Retry in {wait}s")
                    time.sleep(wait)
        return wrapper
    return decorator

# ใช้งาน
@retry(max_attempts=3, exceptions=(ConnectionError, TimeoutError))
def fetch_from_api(url):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()
```

### Connector-level Retry

```python
import psycopg2
from psycopg2 import OperationalError

def get_connection_with_retry(dsn, max_attempts=5):
    """Retry DB connection with exponential backoff"""
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(dsn)
            return conn
        except OperationalError as e:
            if attempt == max_attempts:
                raise
            wait = 2 ** attempt
            logger.warning(f"DB connection failed (attempt {attempt}): {e}")
            time.sleep(wait)
```

---

## Dead Letter Queue (DLQ)

เก็บ records ที่ process ไม่สำเร็จ เพื่อ investigate/reprocess ทีหลัง

### Pattern: DLQ Table

```python
def process_with_dlq(conn, df, table, conflict_columns):
    """Process records ทีละ batch, ส่ง failed rows ไป DLQ"""
    success_count = 0
    failed_rows = []

    for i in range(0, len(df), BATCH_SIZE):
        batch = df.iloc[i:i + BATCH_SIZE]
        try:
            upsert_postgres(conn, batch, table, conflict_columns)
            success_count += len(batch)
        except Exception as e:
            logger.error(f"Batch {i} failed: {e}")
            for _, row in batch.iterrows():
                failed_rows.append({
                    **row.to_dict(),
                    "_error": str(e),
                    "_failed_at": datetime.now().isoformat(),
                })

    # เก็บ failed rows ใน DLQ table
    if failed_rows:
        dlq_df = pd.DataFrame(failed_rows)
        dlq_df.to_sql(f"dlq_{table}", conn, if_exists="append", index=False)
        logger.warning(f"{len(failed_rows)} rows sent to DLQ")

    return success_count, len(failed_rows)
```

### DLQ Schema

```sql
CREATE TABLE dlq_target_table (
    dlq_id          SERIAL PRIMARY KEY,
    -- original columns (as JSONB for flexibility)
    payload         JSONB NOT NULL,
    -- metadata
    error_message   TEXT,
    failed_at       TIMESTAMP DEFAULT NOW(),
    retry_count     INTEGER DEFAULT 0,
    resolved_at     TIMESTAMP,
    resolved_by     TEXT
);

CREATE INDEX idx_dlq_unresolved ON dlq_target_table (failed_at)
    WHERE resolved_at IS NULL;
```

### DLQ Reprocessing

```python
def reprocess_dlq(conn, table):
    """ดึง DLQ records กลับมา process ใหม่"""
    dlq_df = pd.read_sql(f"""
        SELECT payload FROM dlq_{table}
        WHERE resolved_at IS NULL AND retry_count < 3
        ORDER BY failed_at
        LIMIT 1000
    """, conn)

    if dlq_df.empty:
        return

    df = pd.json_normalize(dlq_df["payload"])
    success, failed = process_with_dlq(conn, df, table, conflict_columns)

    # Mark resolved
    conn.execute(f"""
        UPDATE dlq_{table}
        SET resolved_at = NOW(), retry_count = retry_count + 1
        WHERE resolved_at IS NULL AND retry_count < 3
    """)
    conn.commit()
    logger.info(f"DLQ reprocess: {success} success, {failed} still failed")
```

---

## Idempotent Writes

### Problem: Retry ทำให้ duplicate

```python
# ❌ ไม่ idempotent — retry = duplicate
cur.execute("INSERT INTO sales (id, amount) VALUES (1, 100)")

# ✅ Idempotent — retry safe
upsert_postgres(conn, df, "sales", conflict_columns=["id"])
# → INSERT ... ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount
```

### Pattern: Transaction + UPSERT

```python
def idempotent_batch_load(conn, df, table, conflict_columns, partition_col=None, partition_value=None):
    """Atomic idempotent load — safe to retry"""
    try:
        if partition_col and partition_value:
            # DELETE + INSERT ใน transaction เดียว (partition-level idempotent)
            conn.execute(
                f"DELETE FROM {table} WHERE {partition_col} = %s",
                (partition_value,),
            )
        upsert_postgres(conn, df, table, conflict_columns)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
```

### Pattern: Exactly-once with Checkpointing

```python
def load_with_checkpoint(conn, df, table, job_name, batch_id):
    """
    Exactly-once semantics:
    - เก็บ batch_id ที่ process สำเร็จแล้ว
    - ถ้า retry → skip batch ที่ทำไปแล้ว
    """
    # Check if already processed
    result = conn.execute(
        "SELECT 1 FROM etl_checkpoints WHERE job = %s AND batch_id = %s",
        (job_name, batch_id),
    ).fetchone()

    if result:
        logger.info(f"Batch {batch_id} already processed, skipping")
        return

    # Process + checkpoint ใน transaction เดียว
    upsert_postgres(conn, df, table, conflict_columns=["id"])
    conn.execute(
        "INSERT INTO etl_checkpoints (job, batch_id, processed_at) VALUES (%s, %s, NOW())",
        (job_name, batch_id),
    )
    conn.commit()
```

---

## Graceful Shutdown

### Problem: Task ถูก kill ระหว่าง process

```python
import signal
import sys

class GracefulShutdown:
    """Handle SIGTERM/SIGINT gracefully"""
    def __init__(self):
        self.should_stop = False
        signal.signal(signal.SIGTERM, self._handler)
        signal.signal(signal.SIGINT, self._handler)

    def _handler(self, signum, frame):
        logger.warning(f"Received signal {signum}, shutting down gracefully...")
        self.should_stop = True

# ใช้ใน batch processing loop
shutdown = GracefulShutdown()

for i, batch in enumerate(batches):
    if shutdown.should_stop:
        logger.info(f"Graceful stop at batch {i}/{len(batches)}")
        # commit สิ่งที่ทำไปแล้ว
        conn.commit()
        sys.exit(0)

    process_batch(batch)
    conn.commit()  # commit per batch — ไม่ต้อง redo ทั้งหมด
```

### Airflow Task Timeout

```python
# ป้องกัน task ค้างไม่จบ
task = PythonOperator(
    task_id="extract",
    python_callable=extract_fn,
    execution_timeout=timedelta(hours=2),  # kill ถ้า > 2 ชม.
    on_failure_callback=on_failure_callback,
)
```

### Connection Cleanup

```python
# ใช้ context manager เสมอ — cleanup อัตโนมัติ
with PostgresConnector(host, port, db, user, pwd) as pg:
    df = pd.read_sql("SELECT * FROM source", pg.conn)
    upsert_postgres(pg.conn, df, "target", conflict_columns=["id"])
# connection ถูก close อัตโนมัติ ไม่ว่าจะ success หรือ error
```

---

## Error Classification

| Category | ตัวอย่าง | Action | Retry? |
|---|---|---|---|
| **Transient** | Network timeout, DB lock | Retry with backoff | ✅ |
| **Resource** | OOM, Disk full | Alert + manual fix | ❌ |
| **Data** | Invalid schema, NULL PK | Send to DLQ | ❌ |
| **Config** | Wrong credentials, host down | Alert + fix config | ❌ |
| **Logic** | Business rule violation | Fix code + redeploy | ❌ |

### Implementation

```python
class TransientError(Exception):
    """Retry-able errors"""
    pass

class DataError(Exception):
    """Bad data — send to DLQ"""
    pass

class ConfigError(Exception):
    """Configuration problem — alert human"""
    pass

def classify_and_handle(error, batch, conn, table):
    """จัดประเภท error แล้วจัดการตาม category"""
    if isinstance(error, (ConnectionError, TimeoutError)):
        raise TransientError(str(error))  # Airflow จะ retry

    elif isinstance(error, (ValueError, TypeError, KeyError)):
        # Bad data — ส่ง DLQ
        send_to_dlq(conn, batch, table, str(error))
        logger.error(f"Data error, {len(batch)} rows → DLQ: {error}")

    elif isinstance(error, PermissionError):
        raise ConfigError(str(error))  # Alert ทันที

    else:
        raise  # Unknown — let it fail loudly
```

---

## Alerting Integration

ใช้ร่วมกับ `utils/notify.py` (GChat callbacks):

```python
from utils.notify import on_failure_callback, on_retry_callback

default_args = {
    "on_failure_callback": on_failure_callback,   # แจ้งเมื่อ fail สุดท้าย
    "on_retry_callback": on_retry_callback,       # แจ้งเมื่อ retry
}
```

### Custom Alert Levels

```python
def alert_with_severity(context, severity="warning"):
    """Alert ตาม severity level"""
    task_id = context["task_instance"].task_id
    error = context.get("exception", "Unknown")

    if severity == "critical":
        # Production fact table failed — alert ทันที
        send_gchat(f"🚨 CRITICAL: {task_id} failed: {error}", mention_all=True)
    elif severity == "warning":
        # Non-critical — log only
        send_gchat(f"⚠️ WARNING: {task_id} retry: {error}")
```

---

## Circuit Breaker

ป้องกัน cascading failure — หยุด call service ที่ down

```python
import time

class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
        self.state = "closed"  # closed=normal, open=blocking, half-open=testing

    def call(self, func, *args, **kwargs):
        if self.state == "open":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "half-open"
            else:
                raise ConnectionError("Circuit breaker OPEN — service unavailable")

        try:
            result = func(*args, **kwargs)
            if self.state == "half-open":
                self.state = "closed"
                self.failure_count = 0
            return result
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
            raise

# ใช้งาน
api_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=120)

def fetch_data():
    return api_breaker.call(requests.get, "http://api/data", timeout=10)
```

---

## Data Validation

ตรวจสอบ data ก่อน/หลัง load — ป้องกัน bad data เข้า production

```python
def validate_dataframe(df, rules):
    """
    Validate DataFrame ตาม rules ที่กำหนด
    Return: (valid_df, invalid_df)
    """
    mask = pd.Series(True, index=df.index)

    for rule in rules:
        if rule["type"] == "not_null":
            mask &= df[rule["column"]].notna()
        elif rule["type"] == "unique":
            mask &= ~df[rule["column"]].duplicated(keep="first")
        elif rule["type"] == "range":
            mask &= df[rule["column"]].between(rule["min"], rule["max"])
        elif rule["type"] == "regex":
            mask &= df[rule["column"]].str.match(rule["pattern"])

    valid_df = df[mask]
    invalid_df = df[~mask]

    if not invalid_df.empty:
        logger.warning(f"Validation: {len(invalid_df)} invalid rows")

    return valid_df, invalid_df

# ใช้งาน
rules = [
    {"type": "not_null", "column": "id"},
    {"type": "not_null", "column": "amount"},
    {"type": "range", "column": "amount", "min": 0, "max": 1_000_000},
    {"type": "regex", "column": "email", "pattern": r"^[\w.]+@[\w.]+$"},
]

valid_df, invalid_df = validate_dataframe(df, rules)
upsert_postgres(conn, valid_df, "target", conflict_columns=["id"])
if not invalid_df.empty:
    send_to_dlq(conn, invalid_df, "target", "validation_failed")
```

---

## Summary

| Strategy | ใช้เมื่อ | Implementation |
|---|---|---|
| **Retry (Airflow)** | Transient errors | `retries=3, retry_exponential_backoff=True` |
| **Retry (code)** | API/DB calls | `@retry` decorator |
| **DLQ** | Bad data ที่ process ไม่ได้ | DLQ table + reprocess job |
| **Idempotent** | ทุก ETL (mandatory) | UPSERT / DELETE+INSERT per partition |
| **Checkpoint** | Exactly-once, long jobs | `etl_checkpoints` table |
| **Graceful shutdown** | Long-running batch | Signal handler + per-batch commit |
| **Circuit breaker** | External API dependency | State machine (closed/open/half-open) |
| **Data validation** | ก่อน load production | Rule-based + DLQ for invalid |
