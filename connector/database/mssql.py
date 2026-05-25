from __future__ import annotations

import threading
from queue import Empty, Queue
from typing import Any

import pymssql
from loguru import logger


class MSSQLConnector:
    """Microsoft SQL Server connector with connection pooling."""

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
        self._pool: Queue | None = None
        self._pool_size = 0
        self._lock = threading.Lock()

    def _create_connection(self):
        """Create a new raw connection."""
        try:
            return pymssql.connect(
                server=self.host,
                port=str(self.port),
                database=self.database,
                user=self.user,
                password=self.password,
                login_timeout=self.connect_timeout,
            )
        except pymssql.OperationalError as e:
            logger.error(f"Failed to connect to MSSQL at {self.host}:{self.port}/{self.database}: {e}")
            raise ConnectionError(f"MSSQL connection failed: {e}") from e

    def connect(self):
        logger.info(f"Connecting to MSSQL at {self.host}:{self.port}/{self.database} (pool={self.min_connections}-{self.max_connections})")
        self._pool = Queue(maxsize=self.max_connections)
        for _ in range(self.min_connections):
            conn = self._create_connection()
            self._pool.put(conn)
            self._pool_size += 1
        return self

    def close(self):
        if self._pool:
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    conn.close()
                except (Empty, Exception):
                    pass
            self._pool = None
            self._pool_size = 0
            logger.info("MSSQL connection pool closed")

    def get_conn(self):
        """Get a connection from the pool."""
        if self._pool is None:
            raise RuntimeError("Not connected. Call connect() first.")
        try:
            conn = self._pool.get_nowait()
            # Test if connection is still alive
            try:
                conn.cursor().execute("SELECT 1")
            except Exception:
                conn = self._create_connection()
            return conn
        except Empty:
            with self._lock:
                if self._pool_size < self.max_connections:
                    self._pool_size += 1
                    return self._create_connection()
            # Pool exhausted, wait for one
            try:
                return self._pool.get(timeout=self.connect_timeout)
            except Empty:
                raise ConnectionError("MSSQL connection pool exhausted")

    def put_conn(self, conn):
        """Return a connection to the pool."""
        if self._pool and conn:
            try:
                self._pool.put_nowait(conn)
            except Exception:
                conn.close()
                with self._lock:
                    self._pool_size -= 1

    @property
    def conn(self):
        """Get a single connection (for backward compatibility)."""
        return self.get_conn()

    def ping(self) -> bool:
        """Check if the connection is alive."""
        try:
            conn = self.get_conn()
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
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
        except pymssql.Error as e:
            conn.rollback()
            logger.error(f"Execute failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        conn = self.get_conn()
        try:
            with conn.cursor(as_dict=True) as cur:
                cur.execute(query, params)
                return [dict(row) for row in cur.fetchall()]
        except pymssql.Error as e:
            logger.error(f"Fetch all failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def fetch_one(self, query: str, params: tuple | None = None) -> dict[str, Any] | None:
        conn = self.get_conn()
        try:
            with conn.cursor(as_dict=True) as cur:
                cur.execute(query, params)
                row = cur.fetchone()
                return dict(row) if row else None
        except pymssql.Error as e:
            logger.error(f"Fetch one failed: {e}")
            raise
        finally:
            self.put_conn(conn)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
