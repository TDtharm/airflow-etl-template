"""Example: Upload DataFrame as Parquet/CSV to MinIO (S3-compatible)."""

from __future__ import annotations

import tempfile

import pandas as pd

from connector.storage import MinioConnector
from jobs.base import BaseJob
from utils.config import Settings
from utils.logger import log


class UploadMinioExample(BaseJob):
    name = "example_upload_minio"

    def run(self, settings: Settings) -> None:
        # Sample data
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "score": [85.5, 92.0, 78.3],
        })

        bucket = "etl-data"

        with MinioConnector(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure,
        ) as minio:
            # 1) Upload as Parquet
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                df.to_parquet(tmp.name, index=False)
                minio.upload_file(bucket, "example/scores.parquet", tmp.name)
            log.info("Uploaded Parquet to MinIO")

            # 2) Upload as CSV bytes
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            minio.upload_bytes(bucket, "example/scores.csv", csv_bytes, content_type="text/csv")
            log.info("Uploaded CSV to MinIO")

            # 3) Upload as JSON
            json_bytes = df.to_json(orient="records").encode("utf-8")
            minio.upload_bytes(bucket, "example/scores.json", json_bytes, content_type="application/json")
            log.info("Uploaded JSON to MinIO")

            # 4) List objects
            objects = minio.list_objects(bucket, prefix="example/")
            log.info(f"Objects in {bucket}/example/: {objects}")

            # 5) Download and verify
            downloaded = minio.download_bytes(bucket, "example/scores.csv")
            log.info(f"Downloaded {len(downloaded)} bytes from MinIO")
