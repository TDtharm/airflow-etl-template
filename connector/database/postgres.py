from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras
from loguru import logger


class PostgresConnector:
    """PostgreSQL database connector."""

    def __init__(self, host: str, port: int, database: str, user: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self._conn = None

    def connect(self):
        logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database}")
        self._conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )
        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("PostgreSQL connection closed")

    @property
    def conn(self):
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._conn

    def execute(self, query: str, params: tuple | None = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(query, params)
        self.conn.commit()

    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def fetch_one(self, query: str, params: tuple | None = None) -> dict[str, Any] | None:
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            row = cur.fetchone()
            return dict(row) if row else None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
