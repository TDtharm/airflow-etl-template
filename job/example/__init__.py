"""Example upsert jobs for testing database operations."""

from job.example.upsert_postgres_example import UpsertPostgresExample
from job.example.upsert_mssql_example import UpsertMssqlExample
from job.example.upsert_impala_example import UpsertImpalaExample
from job.example.insert_do_nothing_example import InsertDoNothingExample
from job.example.upload_hdfs_example import UploadHDFSExample
from job.example.upload_minio_example import UploadMinioExample

__all__ = [
    "UpsertPostgresExample",
    "UpsertMssqlExample",
    "UpsertImpalaExample",
    "InsertDoNothingExample",
    "UploadHDFSExample",
    "UploadMinioExample",
]
