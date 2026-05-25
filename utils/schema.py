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
    partition_columns: dict[str, str] | None = None,
) -> str:
    """Generate CREATE EXTERNAL TABLE for Impala (Hive-compatible).

    Args:
        partition_columns: Optional dict of {col_name: sql_type} for PARTITIONED BY.
                           e.g. {"year": "INT", "month": "INT"}
    """
    df = _add_audit_columns(df)

    # Remove partition columns from main column list (Hive convention)
    if partition_columns:
        df = df.drop(columns=[c for c in partition_columns if c in df.columns], errors="ignore")

    full_name = f"{database}.{table}"
    ext = "EXTERNAL " if external else ""
    columns = _build_columns(df, IMPALA_DTYPE_MAP)

    partition_clause = ""
    if partition_columns:
        parts = ", ".join(f"{col} {dtype}" for col, dtype in partition_columns.items())
        partition_clause = f"\nPARTITIONED BY ({parts})"

    return f"CREATE {ext}TABLE IF NOT EXISTS {full_name} (\n{columns}\n){partition_clause}\nSTORED AS {stored_as};"


def create_table_kudu(
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    primary_key_columns: list[str] | None = None,
    kudu_master: str = "localhost:7051",
    kudu_replicas: int = 3,
    hash_partition_columns: list[str] | None = None,
    hash_partitions: int = 8,
    range_partition_column: str | None = None,
) -> str:
    """Generate CREATE TABLE ... STORED AS KUDU with primary key and TBLPROPERTIES.

    Args:
        hash_partition_columns: Columns for HASH partitioning.
        hash_partitions: Number of hash buckets (default 8).
        range_partition_column: Column for RANGE partitioning.
    """
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

    # Partition clause
    partition_clause = ""
    if hash_partition_columns:
        hash_cols = ", ".join(hash_partition_columns)
        partition_clause += f"\nPARTITION BY HASH ({hash_cols}) PARTITIONS {hash_partitions}"
    if range_partition_column:
        if partition_clause:
            partition_clause += f",\n    RANGE ({range_partition_column}) ()"
        else:
            partition_clause += f"\nPARTITION BY RANGE ({range_partition_column}) ()"

    return (
        f"CREATE TABLE IF NOT EXISTS {full_name} (\n{columns}{pk_clause}\n){partition_clause}\n"
        f"STORED AS KUDU\n"
        f"TBLPROPERTIES(\n"
        f"    'kudu.master_addresses' = '{kudu_master}',\n"
        f"    'kudu.num_tablet_replicas' = '{kudu_replicas}'\n"
        f");"
    )


# -- Iceberg dtype mapping (same types as Impala but Iceberg uses its own type system) --

ICEBERG_DTYPE_MAP = {
    "int64": "BIGINT",
    "int32": "INT",
    "float64": "DOUBLE",
    "float32": "FLOAT",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "STRING",
}


def create_table_iceberg(
    df: pd.DataFrame,
    table: str,
    database: str = "default",
    partition_spec: list[str] | None = None,
    tblproperties: dict[str, str] | None = None,
    external: bool = False,
) -> str:
    """Generate CREATE TABLE ... STORED AS ICEBERG.

    Iceberg supports ACID, schema evolution, time travel, and hidden partitioning.

    Args:
        partition_spec: Partition transforms, e.g. ["year(insert_date)", "bucket(8, id)"].
                        Iceberg uses hidden partitioning — columns stay in the schema.
        tblproperties: Additional TBLPROPERTIES, e.g. {"write.format.default": "parquet"}.
        external: If True, creates EXTERNAL TABLE (for catalog-managed tables).
    """
    df = _add_audit_columns(df)

    full_name = f"{database}.{table}"
    ext = "EXTERNAL " if external else ""
    columns = _build_columns(df, ICEBERG_DTYPE_MAP)

    partition_clause = ""
    if partition_spec:
        specs = ", ".join(partition_spec)
        partition_clause = f"\nPARTITION BY SPEC ({specs})"

    # Default tblproperties for Iceberg
    props = {
        "write.format.default": "parquet",
    }
    if tblproperties:
        props.update(tblproperties)

    props_str = ",\n".join(f"    '{k}' = '{v}'" for k, v in props.items())
    tbl_props = f"\nTBLPROPERTIES(\n{props_str}\n)"

    return (
        f"CREATE {ext}TABLE IF NOT EXISTS {full_name} (\n{columns}\n)"
        f"{partition_clause}\n"
        f"STORED AS ICEBERG"
        f"{tbl_props};"
    )
