"""Example: Upload DataFrame as Parquet to HDFS (supports LDAP/Kerberos)."""

from __future__ import annotations

import tempfile

import pandas as pd

from connector.storage import HDFSConnector
from job.base import BaseJob
from utils.config import Settings
from utils.logger import log


class UploadHDFSExample(BaseJob):
    name = "example_upload_hdfs"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        # Connect to HDFS (auto-detects auth: PLAIN/LDAP/GSSAPI from settings)
        with HDFSConnector(
            url=settings.hdfs_url,
            user=settings.hdfs_user,
            auth_mechanism=settings.hdfs_auth_mechanism,
            password=settings.hdfs_password,
            kerberos_principal=settings.hdfs_kerberos_principal,
            root=settings.hdfs_root,
        ) as hdfs:
            # 1) Upload DataFrame as Parquet file
            hdfs_path = "/data/example/scores.parquet"
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                df.to_parquet(tmp.name, index=False)
                hdfs.upload_file(hdfs_path, tmp.name)
            log.info(f"Uploaded DataFrame to HDFS: {hdfs_path}")

            # 2) Upload as CSV
            hdfs_csv_path = "/data/example/scores.csv"
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            hdfs.upload_bytes(hdfs_csv_path, csv_bytes)
            log.info(f"Uploaded CSV to HDFS: {hdfs_csv_path}")

            # 3) List files
            files = hdfs.list_dir("/data/example")
            log.info(f"Files in /data/example: {files}")

            # 4) Download and verify
            downloaded = hdfs.download_bytes(hdfs_csv_path)
            log.info(f"Downloaded {len(downloaded)} bytes from {hdfs_csv_path}")
