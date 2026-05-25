from __future__ import annotations

from typing import Any

import psycopg2
import psycopg2.extras
import psycopg2.pool
from loguru import logger


class PostgresConnector:
    """PostgreSQL database connector with connection pooling."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        min_connections: int = 1,
        max_connections: int = 5,
        connect_timeout: int = 10,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connect_timeout = connect_timeout
        self._pool: psycopg2.pool.ThreadedConnectionPool | None = None
        self._conn = None

    def connect(self):
        logger.info(f"Connecting to PostgreSQL at {self.host}:{self.port}/{self.database} (pool={self.min_connections}-{self.max_connections})")
        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.min_connections,
                maxconn=self.max_connections,
                host=self.host,
                port=self.port,
                dbname=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=self.connect_timeout,
            )
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to PostgreSQL at {self.host}:{self.port}/{self.database}: {e}")
            raise ConnectionError(f"PostgreSQL connection failed: {e}") from e
        return self

    def close(self):
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")

    def get_conn(self):
        """Get a connection from the pool."""
        if self._pool is None:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            conn = self._pool.getconn()
        except psycopg2.pool.PoolError as e:
            logger.error(f"Failed to get connection from pool: {e}")
            raise ConnectionError(f"Connection pool exhausted: {e}") from e
        return conn

    def put_conn(self, conn):
        """Return a connection to the pool."""
        if self._pool and conn:
            self._pool.putconn(conn)

    @property
    def conn(self):
        """Get a single connection (for backward compatibility)."""
        if self._conn is None or self._conn.closed:
            self._conn = self.get_conn()
        return self._conn

    def ping(self) -> bool:
        """Check if the connection is alive."""
        try:
            conn = self.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                return True
            finally:
                self.put_conn(conn)
        except Exception:
            return False

    def execute(self, query: str, params: tuple | None = None) -> None:
        conn = self.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            logger.error(f"Execute failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        conn = self.get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
        except psycopg2.Error as e:
            logger.error(f"Fetch all failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def fetch_one(self, query: str, params: tuple | None = None) -> dict[str, Any] | None:
        conn = self.get_conn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None
        except psycopg2.Error as e:
            logger.error(f"Fetch one failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
