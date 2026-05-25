"""Data sync job – logic for syncing data between systems."""

from utils.config import Settings
from jobs.base import BaseJob
from utils.logger import log


class DataSyncJob(BaseJob):
    name = "data_sync"

    def run(self, settings: Settings) -> None:
        # Example:
        # from database import PostgresConnector
        # with PostgresConnector(host=settings.postgres_host, ...) as pg:
        #     rows = pg.fetch_all("SELECT ...")
        log.info(f"[{self.name}] Syncing data (dry-run)")
