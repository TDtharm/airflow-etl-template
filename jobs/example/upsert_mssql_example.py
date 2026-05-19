"""Example: Upsert into MSSQL."""

from __future__ import annotations

import pandas as pd

from connector.database import MSSQLConnector
from jobs.base import BaseJob
from utils.config import Settings
from utils.schema import create_table_mssql
from utils.upsert import upsert_mssql
from utils.logger import log


class UpsertMssqlExample(BaseJob):
    name = "example_upsert_mssql"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        with MSSQLConnector(
            host=settings.mssql_host,
            port=settings.mssql_port,
            database=settings.mssql_db,
            user=settings.mssql_user,
            password=settings.mssql_password,
        ) as mssql:
            # 1) Create table
            ddl = create_table_mssql(df, "example_upsert")
            log.info(f"DDL:\n{ddl}")
            mssql.execute(ddl)

            # Add unique constraint for MERGE to work
            try:
                mssql.execute(
                    "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'UQ_example_upsert_id') "
                    "CREATE UNIQUE INDEX UQ_example_upsert_id ON dbo.example_upsert (id)"
                )
            except Exception:
                pass  # already exists

            # 2) Upsert data
            count = upsert_mssql(
                mssql.conn, df, "example_upsert",
                conflict_columns=["id"],
                insert_by="example_upsert_mssql",
            )
            log.info(f"Upserted {count} rows")

            # 3) Upsert again with updated data
            df["score"] = [90.0, 95.0, 80.0]
            count = upsert_mssql(
                mssql.conn, df, "example_upsert",
                conflict_columns=["id"],
                insert_by="example_upsert_mssql",
            )
            log.info(f"Upserted (updated) {count} rows")

            # 4) Verify
            rows = mssql.fetch_all("SELECT * FROM dbo.example_upsert ORDER BY id")
            for row in rows:
                log.info(row)
