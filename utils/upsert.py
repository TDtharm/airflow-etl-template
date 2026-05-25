"""Upsert (INSERT or UPDATE) operations for PostgreSQL, MSSQL, Impala/Kudu/Parquet, and ClickHouse.

Batch size recommendations:
- PostgreSQL: 5000 (execute_values sends all in one round-trip, network I/O is bottleneck)
- MSSQL:       500 (MERGE is row-by-row, TDS protocol overhead per statement)
- Impala/Kudu: 2000 (Thrift RPC overhead, Kudu tablet flush per batch)
- Impala/Parquet: full DataFrame (INSERT...SELECT from staging is single MapReduce job)
- ClickHouse: 100000 (columnar native format, designed for large batches)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pandas as pd
from loguru import logger

# Optimal batch sizes per database
BATCH_POSTGRES = 5000    # execute_values = 1 round-trip per batch, PG handles large VALUES well
BATCH_MSSQL = 500        # MERGE is per-row, TDS packet size ~4KB, smaller batches reduce lock time
BATCH_KUDU = 2000        # Kudu flushes per batch, sweet spot between RPC calls and memory

# Regex to validate SQL identifiers (table names, column names)
_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_identifier(name: str, kind: str = "identifier") -> str:
    """Validate that a name is a safe SQL identifier to prevent injection."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL {kind}: {name!r}. Must match [a-zA-Z_][a-zA-Z0-9_]*")
    return name


def _validate_identifiers(names: list[str], kind: str = "identifier") -> list[str]:
    """Validate a list of SQL identifiers."""
    for name in names:
        _validate_identifier(name, kind)
    return names


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

    if df.empty:
        logger.warning("[postgres] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    _validate_identifiers(conflict_columns, "conflict column")

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

    if update_columns is None:
        update_columns = [c for c in all_columns if c not in conflict_columns]
    else:
        _validate_identifiers(update_columns, "update column")

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
    except Exception as e:
        conn.rollback()
        logger.error(f"[postgres] upsert failed on {schema}.{table}: {e}")
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

    if df.empty:
        logger.warning("[postgres] insert_do_nothing called with empty DataFrame, skipping")
        return 0

    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    _validate_identifiers(conflict_columns, "conflict column")

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

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
    except Exception as e:
        conn.rollback()
        logger.error(f"[postgres] insert_do_nothing failed on {schema}.{table}: {e}")
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
    if df.empty:
        logger.warning("[mssql] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(schema, "schema")
    _validate_identifier(table, "table")
    _validate_identifiers(conflict_columns, "conflict column")

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

    if update_columns is None:
        update_columns = [c for c in all_columns if c not in conflict_columns]
    else:
        _validate_identifiers(update_columns, "update column")

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
    except Exception as e:
        conn.rollback()
        logger.error(f"[mssql] upsert failed on {target}: {e}")
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
    if df.empty:
        logger.warning("[impala/kudu] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")

    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

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
    except Exception as e:
        logger.error(f"[impala/kudu] upsert failed on {full_name}: {e}")
        raise
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
    if df.empty:
        logger.warning("[impala/parquet] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(key_columns, "key column")

    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

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
    except Exception as e:
        logger.error(f"[impala/parquet] upsert failed on {full_name}: {e}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stg}")
        except Exception:
            pass
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
    if df.empty:
        logger.warning("[impala/parquet] incremental insert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(key_columns, "key column")

    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

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
    except Exception as e:
        logger.error(f"[impala/parquet] incremental insert failed on {full_name}: {e}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stg}")
        except Exception:
            pass
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
    if df.empty:
        logger.warning("[iceberg] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(key_columns, "key column")

    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

    if update_columns is None:
        update_columns = [c for c in all_columns if c not in key_columns]
    else:
        _validate_identifiers(update_columns, "update column")

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
    except Exception as e:
        logger.error(f"[iceberg] upsert failed on {full_name}: {e}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stg}")
        except Exception:
            pass
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
    if df.empty:
        logger.warning("[iceberg] incremental insert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(key_columns, "key column")

    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"
    stg = f"{database}.{_staging_table_name(table)}"

    all_columns = list(df.columns)
    _validate_identifiers(all_columns, "column")

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
    except Exception as e:
        logger.error(f"[iceberg] incremental insert failed on {full_name}: {e}")
        try:
            cur.execute(f"DROP TABLE IF EXISTS {stg}")
        except Exception:
            pass
        raise
    finally:
        cur.close()

    return total


# ---------------------------------------------------------------------------
# ClickHouse  —  ReplacingMergeTree / INSERT + deduplication
# ClickHouse uses ReplacingMergeTree for upsert semantics.
# Rows with same ORDER BY key are deduplicated on background merge or OPTIMIZE FINAL.
# ---------------------------------------------------------------------------

BATCH_CLICKHOUSE = 100_000  # ClickHouse excels at large batches, native format is columnar


# -- Pandas dtype → ClickHouse type mapping --
_PD_TO_CH_TYPE: dict[str, str] = {
    "int8": "Int8",
    "int16": "Int16",
    "int32": "Int32",
    "int64": "Int64",
    "uint8": "UInt8",
    "uint16": "UInt16",
    "uint32": "UInt32",
    "uint64": "UInt64",
    "float16": "Float32",
    "float32": "Float32",
    "float64": "Float64",
    "bool": "Bool",
    "object": "String",
    "string": "String",
    "datetime64[ns]": "DateTime64(3)",
    "datetime64[ns, UTC]": "DateTime64(3, 'UTC')",
    "date": "Date",
}


def _infer_ch_type(dtype) -> str:
    """Infer ClickHouse column type from pandas dtype."""
    dtype_str = str(dtype)
    if dtype_str in _PD_TO_CH_TYPE:
        return _PD_TO_CH_TYPE[dtype_str]
    if dtype_str.startswith("datetime64"):
        return "DateTime64(3)"
    if dtype_str.startswith("float"):
        return "Float64"
    if dtype_str.startswith("int") or dtype_str.startswith("Int"):
        return "Int64"
    if dtype_str.startswith("uint") or dtype_str.startswith("UInt"):
        return "UInt64"
    return "String"


def create_table_clickhouse(
    client,
    df: pd.DataFrame,
    table: str,
    order_by: list[str],
    database: str = "default",
    engine: str = "ReplacingMergeTree",
    version_column: str | None = None,
    partition_by: str | None = None,
    if_not_exists: bool = True,
) -> None:
    """Create a ClickHouse table from DataFrame schema.

    Args:
        client: clickhouse-connect Client instance.
        df: DataFrame to infer schema from.
        table: Table name.
        order_by: Columns for ORDER BY (also used as dedup key in ReplacingMergeTree).
        database: Target database.
        engine: Table engine (ReplacingMergeTree, MergeTree, etc.).
        version_column: Version column for ReplacingMergeTree (keeps row with max version).
        partition_by: Partition expression (e.g. "toYYYYMM(insert_date)").
        if_not_exists: Add IF NOT EXISTS clause.
    """
    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(order_by, "order_by column")

    columns_def = []
    for col in df.columns:
        _validate_identifier(col, "column")
        ch_type = _infer_ch_type(df[col].dtype)
        # Make nullable if column has NaN/None (except order_by columns)
        if col not in order_by and df[col].isna().any():
            ch_type = f"Nullable({ch_type})"
        columns_def.append(f"    {col} {ch_type}")

    columns_sql = ",\n".join(columns_def)
    exists_clause = "IF NOT EXISTS " if if_not_exists else ""

    # Engine clause
    engine_clause = engine
    if engine == "ReplacingMergeTree" and version_column:
        _validate_identifier(version_column, "version_column")
        engine_clause = f"ReplacingMergeTree({version_column})"

    order_by_sql = ", ".join(order_by)
    partition_clause = f"\nPARTITION BY {partition_by}" if partition_by else ""

    sql = (
        f"CREATE TABLE {exists_clause}{database}.{table} (\n"
        f"{columns_sql}\n"
        f") ENGINE = {engine_clause}{partition_clause}\n"
        f"ORDER BY ({order_by_sql})"
    )

    try:
        client.command(sql)
        logger.info(f"[clickhouse] created table {database}.{table} (engine={engine})")
    except Exception as e:
        logger.error(f"[clickhouse] create table failed for {database}.{table}: {e}")
        raise


def upsert_clickhouse(
    client,
    df: pd.DataFrame,
    table: str,
    order_by: list[str],
    database: str = "default",
    insert_by: str = "",
    optimize: bool = False,
) -> int:
    """Upsert DataFrame into ClickHouse ReplacingMergeTree table.

    Strategy:
    - INSERT all rows (ClickHouse accepts duplicates).
    - ReplacingMergeTree deduplicates rows with same ORDER BY key on merge.
    - If optimize=True, run OPTIMIZE FINAL to force immediate dedup (slow on large tables).

    For most use cases, leave optimize=False and let background merges handle dedup.
    Queries should use FINAL modifier: SELECT * FROM table FINAL WHERE ...
    """
    if df.empty:
        logger.warning("[clickhouse] upsert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(order_by, "order_by column")

    df = _add_audit_values(df, insert_by)

    full_name = f"{database}.{table}"
    try:
        client.insert_df(table=table, df=df, database=database)
        total = len(df)
        logger.info(f"[clickhouse] inserted {total} rows into {full_name}")

        if optimize:
            client.command(f"OPTIMIZE TABLE {full_name} FINAL")
            logger.info(f"[clickhouse] OPTIMIZE FINAL on {full_name}")

        return total
    except Exception as e:
        logger.error(f"[clickhouse] upsert failed on {full_name}: {e}")
        raise


def insert_incremental_clickhouse(
    client,
    df: pd.DataFrame,
    table: str,
    key_columns: list[str],
    database: str = "default",
    insert_by: str = "",
) -> int:
    """Incremental insert into ClickHouse — only insert rows with new keys.

    Strategy:
    1. Query existing keys from target table.
    2. Filter DataFrame to only new rows (anti-join on key_columns).
    3. INSERT only new rows.

    Best for append-only tables (MergeTree) where you don't want duplicates.
    """
    if df.empty:
        logger.warning("[clickhouse] incremental insert called with empty DataFrame, skipping")
        return 0

    _validate_identifier(database, "database")
    _validate_identifier(table, "table")
    _validate_identifiers(key_columns, "key column")

    df = _add_audit_values(df, insert_by)
    full_name = f"{database}.{table}"

    # Build anti-join: check which keys already exist
    key_list = ", ".join(key_columns)

    try:
        # Get existing keys for fast lookup
        if len(key_columns) == 1:
            key_col = key_columns[0]
            values = df[key_col].tolist()
            result = client.query(
                f"SELECT DISTINCT {key_col} FROM {full_name} WHERE {key_col} IN %(keys)s",
                parameters={"keys": values},
            )
            existing_keys = {row[0] for row in result.result_rows}
            new_df = df[~df[key_col].isin(existing_keys)]
        else:
            # Multi-column key: fetch existing combos and filter
            result = client.query(f"SELECT DISTINCT {key_list} FROM {full_name}")
            existing_keys = {tuple(row) for row in result.result_rows}
            mask = ~df.set_index(key_columns).index.isin(existing_keys)
            new_df = df[mask.values]

        if new_df.empty:
            logger.info(f"[clickhouse] no new rows to insert into {full_name}")
            return 0

        client.insert_df(table=table, df=new_df, database=database)
        total = len(new_df)
        logger.info(f"[clickhouse] incremental insert {total} new rows into {full_name} (skipped {len(df) - total})")
        return total
    except Exception as e:
        logger.error(f"[clickhouse] incremental insert failed on {full_name}: {e}")
        raise
