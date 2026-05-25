from __future__ import annotations

import time
import traceback
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from utils.logger import log

if TYPE_CHECKING:
    from utils.config import Settings


class JobError(Exception):
    """Raised when a job fails during execution."""

    def __init__(self, job_name: str, message: str, original_error: Exception | None = None):
        self.job_name = job_name
        self.original_error = original_error
        super().__init__(f"[{job_name}] {message}")


class BaseJob(ABC):
    """Base class for a job.

    Subclass and implement run().
    main.py will call run(settings) when --job is specified.
    """

    name: str = "base_job"

    @abstractmethod
    def run(self, settings: Settings) -> None:
        ...

    def on_success(self, elapsed: float) -> None:
        """Hook called after successful run. Override for custom behavior."""
        pass

    def on_failure(self, error: Exception, elapsed: float) -> None:
        """Hook called after failed run. Override for custom behavior (e.g. notify)."""
        pass

    def __call__(self, settings: Settings) -> None:
        log.info(f"[{self.name}] Starting")
        start = time.perf_counter()
        try:
            self.run(settings)
            elapsed = time.perf_counter() - start
            log.info(f"[{self.name}] Done ({elapsed:.2f}s)")
            self.on_success(elapsed)
        except KeyboardInterrupt:
            elapsed = time.perf_counter() - start
            log.warning(f"[{self.name}] Interrupted after {elapsed:.2f}s")
            raise
        except Exception as e:
            elapsed = time.perf_counter() - start
            log.error(f"[{self.name}] Failed after {elapsed:.2f}s: {e}")
            log.debug(f"[{self.name}] Traceback:\n{traceback.format_exc()}")
            self.on_failure(e, elapsed)
            raise JobError(self.name, str(e), original_error=e) from e
