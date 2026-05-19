"""Job registry – register all job classes here."""

from job.data_sync import DataSyncJob
from job.backup import BackupJob
from job.healthcheck import HealthcheckJob
from job.example import (
    UpsertPostgresExample,
    UpsertMssqlExample,
    UpsertImpalaExample,
    InsertDoNothingExample,
    UploadHDFSExample,
    UploadMinioExample,
)

# Central registry: name → job class
JOB_REGISTRY: dict = {
    "data_sync": DataSyncJob,
    "backup": BackupJob,
    "healthcheck": HealthcheckJob,
    "example_upsert_postgres": UpsertPostgresExample,
    "example_upsert_mssql": UpsertMssqlExample,
    "example_upsert_impala": UpsertImpalaExample,
    "example_insert_do_nothing": InsertDoNothingExample,
    "example_upload_hdfs": UploadHDFSExample,
    "example_upload_minio": UploadMinioExample,
}
