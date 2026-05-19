"""Example: Upsert into Impala/Kudu."""

from __future__ import annotations

import pandas as pd

from connector.database import ImpalaConnector
from job.base import BaseJob
from utils.config import Settings
from utils.schema import create_table_kudu
from utils.upsert import upsert_impala
from utils.logger import log


class UpsertImpalaExample(BaseJob):
    name = "example_upsert_impala"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        with ImpalaConnector(
            host=settings.impala_host,
            port=settings.impala_port,
            database=settings.impala_db,
            user=settings.impala_user,
            password=settings.impala_password,
        ) as impala:
            # 1) Create Kudu table with primary key
            ddl = create_table_kudu(
                df, "example_upsert",
                database=settings.impala_db,
                primary_key_columns=["id"],
            )
            log.info(f"DDL:\n{ddl}")
            impala.execute(ddl)

            # 2) Upsert data (Kudu native UPSERT)
            count = upsert_impala(
                impala.conn, df, "example_upsert",
                database=settings.impala_db,
                insert_by="example_upsert_impala",
            )
            log.info(f"Upserted {count} rows")

            # 3) Upsert again with updated data
            df["score"] = [90.0, 95.0, 80.0]
            count = upsert_impala(
                impala.conn, df, "example_upsert",
                database=settings.impala_db,
                insert_by="example_upsert_impala",
            )
            log.info(f"Upserted (updated) {count} rows")

            # 4) Verify
            rows = impala.fetch_all(f"SELECT * FROM {settings.impala_db}.example_upsert ORDER BY id")
            for row in rows:
                log.info(row)
