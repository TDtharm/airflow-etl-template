"""Example: Upsert into PostgreSQL."""

from __future__ import annotations

import pandas as pd

from connector.database import PostgresConnector
from jobs.base import BaseJob
from utils.config import Settings
from utils.schema import create_table_postgres
from utils.upsert import upsert_postgres
from utils.logger import log


class UpsertPostgresExample(BaseJob):
    name = "example_upsert_postgres"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        with PostgresConnector(
            host=settings.postgres_host,
            port=settings.postgres_port,
            database=settings.postgres_db,
            user=settings.postgres_user,
            password=settings.postgres_password,
        ) as pg:
            # 1) Create table with UNIQUE on id
            ddl = create_table_postgres(df, "example_upsert", unique_columns=["id"])
            log.info(f"DDL:\n{ddl}")
            pg.execute(ddl)

            # 2) Upsert data
            count = upsert_postgres(
                pg.conn, df, "example_upsert",
                conflict_columns=["id"],
                insert_by="example_upsert_postgres",
            )
            log.info(f"Upserted {count} rows")

            # 3) Upsert again with updated data (should update, not duplicate)
            df["score"] = [90.0, 95.0, 80.0]
            count = upsert_postgres(
                pg.conn, df, "example_upsert",
                conflict_columns=["id"],
                insert_by="example_upsert_postgres",
            )
            log.info(f"Upserted (updated) {count} rows")

            # 4) Verify
            rows = pg.fetch_all("SELECT * FROM public.example_upsert ORDER BY id")
            for row in rows:
                log.info(row)
