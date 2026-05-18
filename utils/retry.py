from __future__ import annotations

import time
from functools import wraps
from typing import Callable

from utils.logger import log


def retry(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0, exceptions: tuple = (Exception,)):
    """Retry decorator with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts.
        delay: Initial delay between retries in seconds.
        backoff: Multiplier for delay after each retry.
        exceptions: Tuple of exception types to catch.

    Example:
        @retry(max_retries=3, delay=1.0)
        def fetch_data():
            return db.query("SELECT ...")
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        log.error(f"[retry] {func.__name__} failed after {max_retries} attempts: {e}")
                        raise
                    log.warning(f"[retry] {func.__name__} attempt {attempt}/{max_retries} failed: {e}. Retrying in {current_delay:.1f}s...")
                    time.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator
