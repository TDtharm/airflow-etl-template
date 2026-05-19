"""HDFS connector using WebHDFS with LDAP/Kerberos support."""

from __future__ import annotations

from io import BytesIO
from typing import Any

from loguru import logger

try:
    from hdfs import InsecureClient
    from hdfs.ext.kerberos import KerberosClient
except ImportError:
    InsecureClient = None
    KerberosClient = None


class HDFSConnector:
    """HDFS connector via WebHDFS (supports no-auth, LDAP, and Kerberos).

    Auth modes:
        - "PLAIN": InsecureClient with user param (no real auth).
        - "LDAP":  InsecureClient with session auth (user/password basic auth proxy).
        - "GSSAPI": KerberosClient using kinit ticket.
    """

    def __init__(
        self,
        url: str = "http://localhost:9870",
        user: str = "hdfs",
        auth_mechanism: str = "PLAIN",
        password: str = "",
        kerberos_principal: str | None = None,
        root: str = "/",
    ):
        """
        Args:
            url: WebHDFS URL (e.g. http://namenode:9870 or https://namenode:9871 for TLS).
            user: HDFS user (for PLAIN/LDAP).
            auth_mechanism: "PLAIN", "LDAP", or "GSSAPI".
            password: Password for LDAP auth.
            kerberos_principal: Kerberos principal (for GSSAPI). Requires kinit beforehand.
            root: HDFS root path.
        """
        self.url = url
        self.user = user
        self.auth_mechanism = auth_mechanism
        self.password = password
        self.kerberos_principal = kerberos_principal
        self.root = root
        self._client = None

    def connect(self):
        if InsecureClient is None:
            raise ImportError("hdfs package not installed. Run: uv add hdfs[kerberos]")

        logger.info(f"Connecting to HDFS at {self.url} (auth={self.auth_mechanism})")

        if self.auth_mechanism == "GSSAPI":
            if KerberosClient is None:
                raise ImportError("hdfs[kerberos] not installed. Run: uv add hdfs[kerberos]")
            self._client = KerberosClient(
                url=self.url,
                root=self.root,
                mutual_auth="OPTIONAL",
            )
        elif self.auth_mechanism == "LDAP":
            import requests
            session = requests.Session()
            session.auth = (self.user, self.password)
            self._client = InsecureClient(
                url=self.url,
                user=self.user,
                root=self.root,
                session=session,
            )
        else:
            # PLAIN — no auth
            self._client = InsecureClient(
                url=self.url,
                user=self.user,
                root=self.root,
            )
        return self

    def close(self):
        self._client = None
        logger.info("HDFS connection closed")

    @property
    def client(self):
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._client

    def upload_bytes(self, hdfs_path: str, data: bytes, overwrite: bool = True):
        """Upload bytes to HDFS."""
        self.client.write(hdfs_path, data=data, overwrite=overwrite)
        logger.info(f"Uploaded {len(data)} bytes -> {hdfs_path}")

    def upload_file(self, hdfs_path: str, local_path: str, overwrite: bool = True):
        """Upload local file to HDFS."""
        self.client.upload(hdfs_path, local_path, overwrite=overwrite)
        logger.info(f"Uploaded {local_path} -> {hdfs_path}")

    def download_bytes(self, hdfs_path: str) -> bytes:
        """Download file from HDFS as bytes."""
        with self.client.read(hdfs_path) as reader:
            return reader.read()

    def download_file(self, hdfs_path: str, local_path: str):
        """Download file from HDFS to local."""
        self.client.download(hdfs_path, local_path, overwrite=True)
        logger.info(f"Downloaded {hdfs_path} -> {local_path}")

    def list_dir(self, hdfs_path: str) -> list[str]:
        """List files in HDFS directory."""
        return self.client.list(hdfs_path)

    def makedirs(self, hdfs_path: str):
        """Create directories recursively."""
        self.client.makedirs(hdfs_path)

    def delete(self, hdfs_path: str, recursive: bool = False):
        """Delete file or directory."""
        self.client.delete(hdfs_path, recursive=recursive)
        logger.info(f"Deleted {hdfs_path}")

    def status(self, hdfs_path: str) -> dict[str, Any] | None:
        """Get file/directory status. Returns None if not found."""
        try:
            return self.client.status(hdfs_path)
        except Exception:
            return None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
