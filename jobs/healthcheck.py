"""Healthcheck job – logic for service health verification."""

from utils.config import Settings
from jobs.base import BaseJob
from utils.logger import log


class HealthcheckJob(BaseJob):
    name = "healthcheck"

    def run(self, settings: Settings) -> None:
        # Example:
        # from database import PostgresConnector
        # with PostgresConnector(host=settings.postgres_host, ...) as pg:
        #     pg.fetch_one("SELECT 1")
        log.info(f"[{self.name}] Checking services (dry-run)")
