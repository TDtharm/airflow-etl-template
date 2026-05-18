"""Backup job – logic for database/file backups."""

from utils.config import Settings
from job.base import BaseJob
from utils.logger import log


class BackupJob(BaseJob):
    name = "backup"

    def run(self, settings: Settings) -> None:
        # Example:
        # from storage import MinioConnector
        # with MinioConnector(endpoint=settings.minio_endpoint, ...) as minio:
        #     minio.upload_file("backup", "dump.sql", "/tmp/dump.sql")
        log.info(f"[{self.name}] Running backup (dry-run)")
