"""Upsert (INSERT or UPDATE) operations for PostgreSQL, MSSQL, and Impala/Kudu."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
from loguru import logger


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
# ---------------------------------------------------------------------------

def upsert_postgres(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_columns: list[str],
    schema: str = "public",
    update_columns: list[str] | None = None,
    insert_by: str = "",
    batch_size: int = 1000,
) -> int:
    """Upsert DataFrame into PostgreSQL using ON CONFLICT.

    Args:
        conn: psycopg2 connection (or PostgresConnector.conn).
        df: Data to upsert.
        table: Target table name.
        conflict_columns: Columns that form the UNIQUE constraint.
        schema: Database schema.
        update_columns: Columns to update on conflict. Defaults to all non-conflict columns.
        insert_by: Value for insert_by audit column.
        batch_size: Rows per executemany batch.

    Returns:
        Total rows processed.
    """
    df = _add_audit_values(df, insert_by)

    all_columns = list(df.columns)
    if update_columns is None:
        update_columns = [c for c in all_columns if c not in conflict_columns]

    col_list = ", ".join(all_columns)
    placeholders = ", ".join(["%s"] * len(all_columns))
    conflict_list = ", ".join(conflict_columns)
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)

    sql = (
        f"INSERT INTO {schema}.{table} ({col_list})\n"
        f"VALUES ({placeholders})\n"
        f"ON CONFLICT ({conflict_list}) DO UPDATE SET {update_set}"
    )

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = 0

    cur = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(sql, batch)
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
# MSSQL  —  MERGE ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT
# ---------------------------------------------------------------------------

def upsert_mssql(
    conn,
    df: pd.DataFrame,
    table: str,
    conflict_columns: list[str],
    schema: str = "dbo",
    update_columns: list[str] | None = None,
    insert_by: str = "",
    batch_size: int = 1000,
) -> int:
    """Upsert DataFrame into MSSQL using MERGE.

    Args:
        conn: pymssql connection (or MSSQLConnector.conn).
        df: Data to upsert.
        table: Target table name.
        conflict_columns: Columns to match on (JOIN condition).
        schema: Database schema.
        update_columns: Columns to update when matched. Defaults to all non-conflict columns.
        insert_by: Value for insert_by audit column.
        batch_size: Rows per batch.

    Returns:
        Total rows processed.
    """
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

def upsert_impala(
    conn,
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    insert_by: str = "",
    batch_size: int = 1000,
) -> int:
    """Upsert DataFrame into Impala/Kudu using UPSERT INTO.

    Kudu tables support native UPSERT — no conflict columns needed.

    Args:
        conn: impyla connection (or ImpalaConnector.conn).
        df: Data to upsert.
        table: Target table name.
        database: Impala database.
        insert_by: Value for insert_by audit column.
        batch_size: Rows per batch.

    Returns:
        Total rows processed.
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
