from __future__ import annotations

from typing import Any

from impala.dbapi import connect as impala_connect
from loguru import logger


class ImpalaConnector:
    """Apache Impala connector with PLAIN/LDAP/GSSAPI (Kerberos) support."""

    def __init__(
        self,
        host: str,
        port: int,
        database: str = "default",
        auth_mechanism: str = "PLAIN",
        user: str = "",
        password: str = "",
        use_ssl: bool = False,
        kerberos_service_name: str = "impala",
        ca_cert: str | None = None,
    ):
        """
        Args:
            auth_mechanism: "PLAIN" (no auth), "LDAP", or "GSSAPI" (Kerberos).
            kerberos_service_name: Kerberos service name (default "impala").
            use_ssl: Enable TLS/SSL.
            ca_cert: Path to CA cert file for SSL verification.
        """
        self.host = host
        self.port = port
        self.database = database
        self.auth_mechanism = auth_mechanism
        self.user = user
        self.password = password
        self.use_ssl = use_ssl
        self.kerberos_service_name = kerberos_service_name
        self.ca_cert = ca_cert
        self._conn = None

    def connect(self):
        logger.info(f"Connecting to Impala at {self.host}:{self.port}/{self.database} (auth={self.auth_mechanism})")
        connect_kwargs = dict(
            host=self.host,
            port=self.port,
            database=self.database,
            auth_mechanism=self.auth_mechanism,
            user=self.user,
            password=self.password,
            use_ssl=self.use_ssl,
        )
        if self.auth_mechanism == "GSSAPI":
            connect_kwargs["kerberos_service_name"] = self.kerberos_service_name
        if self.ca_cert:
            connect_kwargs["ca_cert"] = self.ca_cert
        self._conn = impala_connect(**connect_kwargs)
        return self

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Impala connection closed")

    @property
    def conn(self):
        if self._conn is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._conn

    def execute(self, query: str, params: tuple | None = None) -> None:
        cur = self.conn.cursor()
        cur.execute(query, params)
        cur.close()

    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        cur = self.conn.cursor()
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        rows = [dict(zip(columns, row)) for row in cur.fetchall()]
        cur.close()
        return rows

    def fetch_one(self, query: str, params: tuple | None = None) -> dict[str, Any] | None:
        cur = self.conn.cursor()
        cur.execute(query, params)
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        cur.close()
        return dict(zip(columns, row)) if row else None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
