"""Job registry – register all job classes here."""

from job.data_sync import DataSyncJob
from job.backup import BackupJob
from job.healthcheck import HealthcheckJob

# Central registry: name → job class
JOB_REGISTRY: dict = {
    "data_sync": DataSyncJob,
    "backup": BackupJob,
    "healthcheck": HealthcheckJob,
}
