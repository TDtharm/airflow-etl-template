from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from utils.logger import log

if TYPE_CHECKING:
    from utils.config import Settings


class BaseJob(ABC):
    """Base class for a job.

    Subclass and implement run().
    main.py will call run(settings) when --job is specified.
    """

    name: str = "base_job"

    @abstractmethod
    def run(self, settings: Settings) -> None:
        ...

    def __call__(self, settings: Settings) -> None:
        log.info(f"[{self.name}] Starting")
        self.run(settings)
        log.info(f"[{self.name}] Done")
