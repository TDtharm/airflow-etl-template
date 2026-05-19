"""Example upsert jobs for testing database operations."""

from jobs.example.upsert_postgres_example import UpsertPostgresExample
from jobs.example.upsert_mssql_example import UpsertMssqlExample
from jobs.example.upsert_impala_example import UpsertImpalaExample
from jobs.example.insert_do_nothing_example import InsertDoNothingExample
from jobs.example.upload_hdfs_example import UploadHDFSExample
from jobs.example.upload_minio_example import UploadMinioExample

__all__ = [
    "UpsertPostgresExample",
    "UpsertMssqlExample",
    "UpsertImpalaExample",
    "InsertDoNothingExample",
    "UploadHDFSExample",
    "UploadMinioExample",
]
