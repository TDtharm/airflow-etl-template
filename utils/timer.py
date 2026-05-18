from __future__ import annotations

import time
from functools import wraps
from typing import Callable

from utils.logger import log


def timer(func: Callable) -> Callable:
    """Decorator that logs execution time of a function.

    Example:
        @timer
        def my_job():
            ...
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        log.info(f"[timer] {func.__name__} took {elapsed:.2f}s")
        return result
    return wrapper
