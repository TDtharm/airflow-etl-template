"""Example: Incremental insert into PostgreSQL (INSERT ... ON CONFLICT DO NOTHING)."""

from __future__ import annotations

import pandas as pd

from connector.database import PostgresConnector
from jobs.base import BaseJob
from utils.config import Settings
from utils.schema import create_table_postgres
from utils.upsert import insert_do_nothing_postgres
from utils.logger import log


class InsertDoNothingExample(BaseJob):
    name = "example_insert_do_nothing"

    def run(self, settings: Settings) -> None:
        # Sample data - batch 1
        df1 = pd.DataFrame({
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
            ddl = create_table_postgres(df1, "example_incremental", unique_columns=["id"])
            log.info(f"DDL:\n{ddl}")
            pg.execute(ddl)

            # 2) Insert first batch
            count = insert_do_nothing_postgres(
                pg.conn, df1, "example_incremental",
                conflict_columns=["id"],
                insert_by="example_incremental",
            )
            log.info(f"Inserted {count} rows (batch 1)")

            # 3) Insert again with overlapping + new data
            #    id 2,3 already exist → skipped, id 4,5 → inserted
            df2 = pd.DataFrame({
                "id": [2, 3, 4, 5],
                "name": ["Bob", "Charlie", "David", "Eve"],
                "score": [99.0, 99.0, 88.0, 91.0],
            })
            count = insert_do_nothing_postgres(
                pg.conn, df2, "example_incremental",
                conflict_columns=["id"],
                insert_by="example_incremental",
            )
            log.info(f"Inserted {count} rows (batch 2 — duplicates skipped)")

            # 4) Verify — id 2,3 should keep original scores (92.0, 78.3)
            rows = pg.fetch_all("SELECT * FROM public.example_incremental ORDER BY id")
            for row in rows:
                log.info(row)
