"""Generate CREATE TABLE statements from pandas DataFrame for each database."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

# -- dtype mapping per database --

POSTGRES_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INTEGER",
    "float64": "DOUBLE PRECISION",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "VARCHAR",
}

MSSQL_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "FLOAT",
    "float32": "REAL",
    "bool": "BIT",
    "datetime64[ns]": "DATETIME2",
    "object": "NVARCHAR(MAX)",
}

IMPALA_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "FLOAT",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
}

KUDU_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "FLOAT",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
}


def _map_dtype(dtype: str, mapping: dict[str, str]) -> str:
    dtype_str = str(dtype)
    if dtype_str in mapping:
        return mapping[dtype_str]
    if dtype_str.startswith("datetime64"):
        return mapping.get("datetime64[ns]", "TIMESTAMP")
    return mapping.get("object", "TEXT")


def _build_columns(df: pd.DataFrame, mapping: dict[str, str], column_overrides: dict[str, str] | None = None) -> str:
    cols = []
    for col_name, dtype in df.dtypes.items():
        if column_overrides and col_name in column_overrides:
            sql_type = column_overrides[col_name]
        elif col_name in ("geom", "geometry"):
            sql_type = "GEOMETRY"
        else:
            sql_type = _map_dtype(dtype, mapping)
        cols.append(f"    {col_name} {sql_type}")
    return ",\n".join(cols)


def _add_audit_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add insert_date (UTC) and insert_by columns if missing."""
    df = df.copy()
    if "insert_date" not in df.columns:
        df["insert_date"] = datetime.now(timezone.utc)
        df["insert_date"] = pd.to_datetime(df["insert_date"], utc=True)
    if "insert_by" not in df.columns:
        df["insert_by"] = ""
    return df


def create_table_postgres(
    df: pd.DataFrame,
    table: str,
    schema: str = "public",
    database: str | None = None,
    if_not_exists: bool = True,
    column_overrides: dict[str, str] | None = None,
    unique_columns: list[str] | None = None,
) -> str:
    """Generate CREATE TABLE for PostgreSQL.

    Args:
        column_overrides: Override auto-detected types, e.g. {"lat": "VARCHAR", "geom": "GEOMETRY"}.
        unique_columns: Columns for UNIQUE constraint (for upsert with ON CONFLICT).
    """
    df = _add_audit_columns(df)
    full_name = f"{database}.{schema}.{table}" if database else f"{schema}.{table}"
    exists = "IF NOT EXISTS " if if_not_exists else ""
    columns = _build_columns(df, POSTGRES_DTYPE_MAP, column_overrides)
    # insert_date gets a server-side UTC default
    columns = columns.replace(
        "insert_date TIMESTAMP",
        "insert_date TIMESTAMP DEFAULT (now() AT TIME ZONE 'UTC')",
    )
    unique = ""
    if unique_columns:
        unique = f",\n    UNIQUE ({', '.join(unique_columns)})"
    return f"CREATE TABLE {exists}{full_name} (\n{columns}{unique}\n);"


def create_table_mssql(df: pd.DataFrame, table: str, schema: str = "dbo", if_not_exists: bool = True) -> str:
    """Generate CREATE TABLE for MSSQL."""
    df = _add_audit_columns(df)
    full_name = f"{schema}.{table}"
    columns = _build_columns(df, MSSQL_DTYPE_MAP)
    stmt = f"CREATE TABLE {full_name} (\n{columns}\n);"
    if if_not_exists:
        return (
            f"IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES "
            f"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}')\n"
            f"{stmt}"
        )
    return stmt


def create_table_impala(
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    stored_as: str = "PARQUET",
    external: bool = True,
) -> str:
    """Generate CREATE EXTERNAL TABLE for Impala (Hive-compatible)."""
    df = _add_audit_columns(df)

    full_name = f"{database}.{table}"
    ext = "EXTERNAL " if external else ""
    columns = _build_columns(df, IMPALA_DTYPE_MAP)
    return f"CREATE {ext}TABLE IF NOT EXISTS {full_name} (\n{columns}\n)\nSTORED AS {stored_as};"


def create_table_kudu(
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    primary_key_columns: list[str] | None = None,
    kudu_master: str = "localhost:7051",
    kudu_replicas: int = 3,
) -> str:
    """Generate CREATE TABLE ... STORED AS KUDU with primary key and TBLPROPERTIES."""
    df = _add_audit_columns(df)

    # reorder columns so primary key columns come first
    if primary_key_columns:
        rest = [c for c in df.columns if c not in primary_key_columns]
        df = df[primary_key_columns + rest]

    full_name = f"{database}.{table}"
    columns = _build_columns(df, KUDU_DTYPE_MAP)

    pk_clause = ""
    if primary_key_columns:
        pk_clause = f",\n    PRIMARY KEY ({', '.join(primary_key_columns)})"

    return (
        f"CREATE TABLE IF NOT EXISTS {full_name} (\n{columns}{pk_clause}\n)\n"
        f"STORED AS KUDU\n"
        f"TBLPROPERTIES(\n"
        f"    'kudu.master_addresses' = '{kudu_master}',\n"
        f"    'kudu.num_tablet_replicas' = '{kudu_replicas}'\n"
        f");"
    )
