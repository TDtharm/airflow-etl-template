from __future__ import annotations

from io import BytesIO
from typing import BinaryIO

from minio import Minio
from loguru import logger


class MinioConnector:
    """MinIO / S3-compatible object storage connector."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool = False):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure
        self._client: Minio | None = None

    def connect(self):
        logger.info(f"Connecting to MinIO at {self.endpoint}")
        self._client = Minio(
            endpoint=self.endpoint,
            access_key=self.access_key,
            secret_key=self.secret_key,
            secure=self.secure,
        )
        return self

    def close(self):
        self._client = None
        logger.info("MinIO connection closed")

    @property
    def client(self) -> Minio:
        if self._client is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._client

    def ensure_bucket(self, bucket: str):
        if not self.client.bucket_exists(bucket):
            self.client.make_bucket(bucket)
            logger.info(f"Created bucket '{bucket}'")

    def upload_bytes(self, bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream"):
        self.ensure_bucket(bucket)
        stream = BytesIO(data)
        self.client.put_object(bucket, object_name, stream, length=len(data), content_type=content_type)
        logger.info(f"Uploaded {object_name} to {bucket}")

    def upload_file(self, bucket: str, object_name: str, file_path: str):
        self.ensure_bucket(bucket)
        self.client.fput_object(bucket, object_name, file_path)
        logger.info(f"Uploaded {file_path} -> {bucket}/{object_name}")

    def download_bytes(self, bucket: str, object_name: str) -> bytes:
        response = self.client.get_object(bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def download_file(self, bucket: str, object_name: str, file_path: str):
        self.client.fget_object(bucket, object_name, file_path)
        logger.info(f"Downloaded {bucket}/{object_name} -> {file_path}")

    def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        return [obj.object_name for obj in self.client.list_objects(bucket, prefix=prefix, recursive=True)]

    def delete_object(self, bucket: str, object_name: str):
        self.client.remove_object(bucket, object_name)

    def list_buckets(self) -> list[str]:
        return [b.name for b in self.client.list_buckets()]

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
