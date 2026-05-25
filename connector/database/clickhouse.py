from __future__ import annotations

from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import ClickHouseError
from loguru import logger


class ClickHouseConnector:
    """ClickHouse database connector with connection pooling via clickhouse-connect."""

    def __init__(
        self,
        host: str,
        port: int = 8123,
        database: str = "default",
        user: str = "default",
        password: str = "",
        secure: bool = False,
        connect_timeout: int = 10,
        send_receive_timeout: int = 300,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.secure = secure
        self.connect_timeout = connect_timeout
        self.send_receive_timeout = send_receive_timeout
        self._client: Client | None = None

    def connect(self):
        logger.info(f"Connecting to ClickHouse at {self.host}:{self.port}/{self.database}")
        try:
            self._client = clickhouse_connect.get_client(
                host=self.host,
                port=self.port,
                database=self.database,
                username=self.user,
                password=self.password,
                secure=self.secure,
                connect_timeout=self.connect_timeout,
                send_receive_timeout=self.send_receive_timeout,
            )
            # Verify connection
            self._client.ping()
        except (ClickHouseError, OSError) as e:
            logger.error(f"Failed to connect to ClickHouse at {self.host}:{self.port}/{self.database}: {e}")
            self._client = None
            raise ConnectionError(f"ClickHouse connection failed: {e}") from e
        return self

    def close(self):
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None
            logger.info("ClickHouse connection closed")

    @property
    def client(self) -> Client:
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._client

    def ping(self) -> bool:
        """Check if the connection is alive."""
        try:
            return self.client.ping()
        except Exception:
            return False

    def execute(self, query: str, params: dict | None = None) -> None:
        """Execute a command (INSERT, CREATE, ALTER, etc.)."""
        try:
            self.client.command(query, parameters=params or {})
        except ClickHouseError as e:
            logger.error(f"Execute failed: {e}")
            raise

    def fetch_all(self, query: str, params: dict | None = None) -> list[dict[str, Any]]:
        """Execute a SELECT query and return all rows as list of dicts."""
        try:
            result = self.client.query(query, parameters=params or {})
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        except ClickHouseError as e:
            logger.error(f"Fetch all failed: {e}")
            raise

    def fetch_one(self, query: str, params: dict | None = None) -> dict[str, Any] | None:
        """Execute a SELECT query and return first row as dict."""
        try:
            result = self.client.query(query, parameters=params or {})
            columns = result.column_names
            if result.result_rows:
                return dict(zip(columns, result.result_rows[0]))
            return None
        except ClickHouseError as e:
            logger.error(f"Fetch one failed: {e}")
            raise

    def insert_df(self, table: str, df, database: str | None = None) -> None:
        """Insert a pandas DataFrame into a ClickHouse table.

        Uses native format for high-performance bulk inserts.
        """
        import pandas as pd

        if not isinstance(df, pd.DataFrame):
            raise TypeError("df must be a pandas DataFrame")
        if df.empty:
            logger.warning(f"[clickhouse] insert_df called with empty DataFrame for {table}, skipping")
            return

        target_db = database or self.database
        try:
            self.client.insert_df(table=table, df=df, database=target_db)
            logger.info(f"[clickhouse] inserted {len(df)} rows into {target_db}.{table}")
        except ClickHouseError as e:
            logger.error(f"[clickhouse] insert_df failed on {target_db}.{table}: {e}")
            raise

    def insert_rows(
        self,
        table: str,
        rows: list[list | tuple],
        column_names: list[str],
        database: str | None = None,
    ) -> None:
        """Insert raw rows into a ClickHouse table."""
        if not rows:
            logger.warning(f"[clickhouse] insert_rows called with empty data for {table}, skipping")
            return

        target_db = database or self.database
        try:
            self.client.insert(
                table=table,
                data=rows,
                column_names=column_names,
                database=target_db,
            )
            logger.info(f"[clickhouse] inserted {len(rows)} rows into {target_db}.{table}")
        except ClickHouseError as e:
            logger.error(f"[clickhouse] insert_rows failed on {target_db}.{table}: {e}")
            raise

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
