"""Upsert (INSERT or UPDATE) operations for PostgreSQL, MSSQL, and Impala/Kudu/Parquet.

Batch size recommendations:
- PostgreSQL: 5000 (execute_values sends all in one round-trip, network I/O is bottleneck)
- MSSQL:       500 (MERGE is row-by-row, TDS protocol overhead per statement)
- Impala/Kudu: 2000 (Thrift RPC overhead, Kudu tablet flush per batch)
- Impala/Parquet: full DataFrame (INSERT...SELECT from staging is single MapReduce job)
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from loguru import logger

# Optimal batch sizes per database
BATCH_POSTGRES = 5000    # execute_values = 1 round-trip per batch, PG handles large VALUES well
BATCH_MSSQL = 500        # MERGE is per-row, TDS packet size ~4KB, smaller batches reduce lock time
BATCH_KUDU = 2000        # Kudu flushes per batch, sweet spot between RPC calls and memory


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _add_audit_values(df: pd.DataFrame, insert_by: str = "") -> pd.DataFrame:
    """Ensure insert_date (UTC) and insert_by columns exist with values."""
    df = df.copy()
    if "insert_date" not in df.columns:
        df["insert_date"] = _now_utc()
    if "insert_by" not in df.columns:
        df["insert_by"] = insert_by
    return df


# ---------------------------------------------------------------------------
# PostgreSQL  —  INSERT ... ON CONFLICT (...) DO UPDATE SET ...
# Uses psycopg2.extras.execute_values (10x faster than executemany)
# ---------------------------------------------------------------------------

def upsert_postgres(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_columns: list[str],
    schema: str = "public",
    update_columns: list[str] | None = None,
    insert_by: str = "",
    batch_size: int = BATCH_POSTGRES,
) -> int:
    """Upsert DataFrame into PostgreSQL using ON CONFLICT.

    Uses execute_values for bulk performance (~10x faster than executemany).
    """
    from psycopg2.extras import execute_values

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    if update_columns is None:
        update_columns = [c for c in all_columns if c not in conflict_columns]

    col_list = ", ".join(all_columns)
    conflict_list = ", ".join(conflict_columns)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    sql = (
        f"INSERT INTO {schema}.{table} ({col_list}) VALUES %s\n"
        f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_set}"
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            execute_values(cur, sql, batch, page_size=batch_size)
            total += len(batch)
        conn.commit()
        logger.info(f"[postgres] upserted {total} rows into {schema}.{table}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    return total


# ---------------------------------------------------------------------------
# PostgreSQL  —  INSERT ... ON CONFLICT (...) DO NOTHING  (incremental insert)
# ---------------------------------------------------------------------------

def insert_do_nothing_postgres(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_columns: list[str],
    schema: str = "public",
    insert_by: str = "",
    batch_size: int = BATCH_POSTGRES,
) -> int:
    """Incremental insert into PostgreSQL — skip rows that already exist.

    Uses execute_values + ON CONFLICT DO NOTHING.
    """
    from psycopg2.extras import execute_values

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    col_list = ", ".join(all_columns)
    conflict_list = ", ".join(conflict_columns)

    sql = (
        f"INSERT INTO {schema}.{table} ({col_list}) VALUES %s\n"
        f"ON CONFLICT ({conflict_list}) DO NOTHING"
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            execute_values(cur, sql, batch, page_size=batch_size)
            total += len(batch)
        conn.commit()
        logger.info(f"[postgres] insert_do_nothing {total} rows into {schema}.{table}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    return total

def upsert_mssql(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_columns: list[str],
    schema: str = "dbo",
    update_columns: list[str] | None = None,
    insert_by: str = "",
    batch_size: int = BATCH_MSSQL,
) -> int:
    """Upsert DataFrame into MSSQL using MERGE."""
    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    if update_columns is None:
        update_columns = [c for c in all_columns if c not in conflict_columns]

    target = f"{schema}.{table}"
    join_on = " AND ".join(f"target.{c} = source.{c}" for c in conflict_columns)
    update_set = ", ".join(f"target.{c} = source.{c}" for c in update_columns)
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join(f"source.{c}" for c in all_columns)
    source_cols = ", ".join(f"%s AS {c}" for c in all_columns)

    sql = (
        f"MERGE {target} AS target\n"
        f"USING (SELECT {source_cols}) AS source\n"
        f"ON {join_on}\n"
        f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});"
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                cur.execute(sql, row)
            total += len(batch)
        conn.commit()
        logger.info(f"[mssql] upserted {total} rows into {target}")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

    return total


# ---------------------------------------------------------------------------
# Impala/Kudu  —  UPSERT INTO ... VALUES (...)
# ---------------------------------------------------------------------------

def upsert_kudu_impala(
    conn,
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    insert_by: str = "",
    batch_size: int = BATCH_KUDU,
) -> int:
    """Upsert DataFrame into Impala/Kudu using UPSERT INTO.

    Kudu tables support native UPSERT — no conflict columns needed.
    """
    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    full_name = f"{database}.{table}"
    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))

    sql = f"UPSERT INTO {full_name} ({col_list}) VALUES ({placeholders})"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                cur.execute(sql, row)
            total += len(batch)
        logger.info(f"[impala/kudu] upserted {total} rows into {full_name}")
    finally:
        cur.close()

    return total


# Alias for convenience
upsert_impala = upsert_kudu_impala


# ---------------------------------------------------------------------------
# Impala/Parquet  —  Fast bulk via staging table + INSERT OVERWRITE / INSERT INTO
# Parquet is immutable: can't UPDATE in-place, must rewrite partition or use staging.
# ---------------------------------------------------------------------------

def _staging_table_name(table: str) -> str:
    return f"_stg_{table}_{int(_now_utc().timestamp())}"


def upsert_parquet_impala(
    conn,
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
    database: str = "default",
    insert_by: str = "",
) -> int:
    """Upsert into Impala Parquet table using staging + INSERT OVERWRITE.

    Strategy (fast — single MapReduce job per step):
    1. CREATE TEMP staging table (Parquet)
    2. INSERT new data into staging
    3. INSERT OVERWRITE target = (staging UNION ALL target rows not in staging)

    This rewrites the entire table — best for small-medium tables or partitioned tables.
    """
    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))
    key_join = " AND ".join(f"t.{k} = s.{k}" for k in key_columns)

    cur = conn.cursor()
    try:
        # 1) Create staging as clone of target
        cur.execute(f"CREATE TABLE {stg} LIKE {full_name} STORED AS PARQUET")

        # 2) Insert new data into staging
        sql = f"INSERT INTO {stg} ({col_list}) VALUES ({placeholders})"
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        for row in rows:
            cur.execute(sql, row)

        # 3) INSERT OVERWRITE target = staging + (target LEFT ANTI JOIN staging)
        cur.execute(
            f"INSERT OVERWRITE {full_name} "
            f"SELECT {col_list} FROM {stg} "
            f"UNION ALL "
            f"SELECT t.* FROM {full_name} t "
            f"LEFT ANTI JOIN {stg} s ON {key_join}"
        )

        # 4) Cleanup staging
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        logger.info(f"[impala/parquet] upserted {len(rows)} rows into {full_name}")
        return len(rows)
    except Exception:
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        raise
    finally:
        cur.close()


def insert_incremental_parquet_impala(
    conn,
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
    database: str = "default",
    insert_by: str = "",
) -> int:
    """Incremental insert into Impala Parquet — only insert rows with new keys.

    Strategy (fast — single INSERT...SELECT):
    1. CREATE staging table with new data
    2. INSERT INTO target SELECT from staging WHERE keys NOT IN target

    Existing rows are untouched (like DO NOTHING).
    """
    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))
    key_join = " AND ".join(f"t.{k} = s.{k}" for k in key_columns)

    cur = conn.cursor()
    try:
        # 1) Create staging
        cur.execute(f"CREATE TABLE {stg} LIKE {full_name} STORED AS PARQUET")

        # 2) Insert all new data into staging
        sql = f"INSERT INTO {stg} ({col_list}) VALUES ({placeholders})"
        rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
        for row in rows:
            cur.execute(sql, row)

        # 3) Insert only new keys into target (LEFT ANTI JOIN = not exists)
        cur.execute(
            f"INSERT INTO {full_name} "
            f"SELECT s.* FROM {stg} s "
            f"LEFT ANTI JOIN {full_name} t ON {key_join}"
        )

        # 4) Cleanup
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        logger.info(f"[impala/parquet] incremental insert {len(rows)} rows into {full_name}")
        return len(rows)
    except Exception:
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        raise
    finally:
        cur.close()


# ---------------------------------------------------------------------------
# Iceberg  —  MERGE INTO (native ACID, CDP 7.1.3+)
# No staging needed — Iceberg supports row-level MERGE natively.
# ---------------------------------------------------------------------------

def upsert_iceberg(
    conn,
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
    database: str = "default",
    update_columns: list[str] | None = None,
    insert_by: str = "",
    batch_size: int = BATCH_KUDU,
) -> int:
    """Upsert DataFrame into Iceberg table using MERGE INTO.

    Native ACID on Cloudera CDP 7.1.3+ — no staging table required.
    Uses a temporary VALUES clause as source for the MERGE.

    Args:
        conn: impyla connection.
        df: Data to upsert.
        table: Target table name.
        key_columns: Columns to match on (JOIN condition).
        database: Impala database.
        update_columns: Columns to update when matched. Defaults to all non-key columns.
        insert_by: Value for insert_by audit column.
        batch_size: Rows per MERGE batch.
    """
    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    if update_columns is None:
        update_columns = [c for c in all_columns if c not in key_columns]

    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))
    join_on = " AND ".join(f"t.{k} = s.{k}" for k in key_columns)
    update_set = ", ".join(f"t.{c} = s.{c}" for c in update_columns)
    insert_vals = ", ".join(f"s.{c}" for c in all_columns)

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        # Use staging table approach for large batches (Impala VALUES clause has limits)
        cur.execute(f"CREATE TABLE IF NOT EXISTS {stg} LIKE {full_name}")

        # Insert data into staging in batches
        insert_sql = f"INSERT INTO {stg} ({col_list}) VALUES ({placeholders})"
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                cur.execute(insert_sql, row)

        # MERGE INTO target from staging
        merge_sql = (
            f"MERGE INTO {full_name} t\n"
            f"USING {stg} s\n"
            f"ON {join_on}\n"
            f"WHEN MATCHED THEN UPDATE SET {update_set}\n"
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals})"
        )
        cur.execute(merge_sql)
        total = len(rows)

        # Cleanup
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        logger.info(f"[iceberg] upserted {total} rows into {full_name}")
    except Exception:
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        raise
    finally:
        cur.close()

    return total


def insert_incremental_iceberg(
    conn,
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
    database: str = "default",
    insert_by: str = "",
    batch_size: int = BATCH_KUDU,
) -> int:
    """Incremental insert into Iceberg — skip rows that already exist.

    Uses MERGE INTO ... WHEN NOT MATCHED THEN INSERT (no UPDATE on match).
    """
    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))
    join_on = " AND ".join(f"t.{k} = s.{k}" for k in key_columns)
    insert_vals = ", ".join(f"s.{c}" for c in all_columns)

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {stg} LIKE {full_name}")

        insert_sql = f"INSERT INTO {stg} ({col_list}) VALUES ({placeholders})"
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            for row in batch:
                cur.execute(insert_sql, row)

        # MERGE — only insert new rows, skip existing
        merge_sql = (
            f"MERGE INTO {full_name} t\n"
            f"USING {stg} s\n"
            f"ON {join_on}\n"
            f"WHEN NOT MATCHED THEN INSERT ({col_list}) VALUES ({insert_vals})"
        )
        cur.execute(merge_sql)
        total = len(rows)

        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        logger.info(f"[iceberg] incremental insert {total} rows into {full_name}")
    except Exception:
        cur.execute(f"DROP TABLE IF EXISTS {stg}")
        raise
    finally:
        cur.close()

    return total
