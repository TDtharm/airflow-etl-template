"""Example: Create table, upsert, and incremental insert into ClickHouse."""

from __future__ import annotations

import pandas as pd

from connector.database import ClickHouseConnector
from jobs.base import BaseJob
from utils.config import Settings
from utils.upsert import (
    create_table_clickhouse,
    upsert_clickhouse,
    insert_incremental_clickhouse,
)
from utils.logger import log


class UpsertClickHouseExample(BaseJob):
    name = "example_upsert_clickhouse"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        with ClickHouseConnector(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            secure=settings.clickhouse_secure,
        ) as ch:
            # 1) Create table (ReplacingMergeTree — dedup by id)
            create_table_clickhouse(
                ch.client, df, "example_upsert",
                order_by=["id"],
                engine="ReplacingMergeTree",
                partition_by="intDiv(id, 100)",
            )

            # 2) Upsert data
            count = upsert_clickhouse(
                ch.client, df, "example_upsert",
                order_by=["id"],
                insert_by="example_upsert_clickhouse",
            )
            log.info(f"Upserted {count} rows")

            # 3) Upsert again with updated scores (ReplacingMergeTree keeps latest)
            df["score"] = [90.0, 95.0, 80.0]
            count = upsert_clickhouse(
                ch.client, df, "example_upsert",
                order_by=["id"],
                insert_by="example_upsert_clickhouse",
                optimize=True,  # Force dedup immediately for demo
            )
            log.info(f"Upserted (updated) {count} rows")

            # 4) Verify (FINAL forces dedup read)
            rows = ch.fetch_all("SELECT * FROM example_upsert FINAL ORDER BY id")
            for row in rows:
                log.info(row)


class IncrementalClickHouseExample(BaseJob):
    name = "example_incremental_clickhouse"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        with ClickHouseConnector(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            secure=settings.clickhouse_secure,
        ) as ch:
            # 1) Create table (MergeTree — append only, no dedup)
            create_table_clickhouse(
                ch.client, df, "example_incremental",
                order_by=["id"],
                engine="MergeTree",
            )

            # 2) First insert — all 3 rows are new
            count = insert_incremental_clickhouse(
                ch.client, df, "example_incremental",
                key_columns=["id"],
                insert_by="example_incremental_clickhouse",
            )
            log.info(f"First insert: {count} new rows")

            # 3) Second insert — ids 1-3 exist, only id 4-5 are new
            df2 = pd.DataFrame({
                "id": [2, 3, 4, 5],
                "name": ["Bob", "Charlie", "Dave", "Eve"],
                "score": [92.0, 78.3, 88.0, 91.5],
            })
            count = insert_incremental_clickhouse(
                ch.client, df2, "example_incremental",
                key_columns=["id"],
                insert_by="example_incremental_clickhouse",
            )
            log.info(f"Second insert: {count} new rows (2 skipped)")

            # 4) Verify
            rows = ch.fetch_all("SELECT * FROM example_incremental ORDER BY id")
            for row in rows:
                log.info(row)
