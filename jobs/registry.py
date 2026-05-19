"""Job registry – register all job classes here."""

from jobs.data_sync import DataSyncJob
from jobs.backup import BackupJob
from jobs.healthcheck import HealthcheckJob
from jobs.example import (
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
