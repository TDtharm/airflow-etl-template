from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
from utils.logger import log


def read_json(path: str | Path) -> list[dict[str, Any]]:
    """Read a JSON file into list[dict]."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, data: list[dict[str, Any]], **kwargs) -> None:
    """Write list[dict] to a JSON file."""
    _ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str, **kwargs)
    log.info(f"[file] Wrote JSON: {path}")


def read_csv(path: str | Path, delimiter: str = ",") -> list[dict[str, Any]]:
    """Read a CSV file into list[dict]."""
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter=delimiter))


def write_csv(path: str | Path, data: list[dict[str, Any]], delimiter: str = ",") -> None:
    """Write list[dict] to a CSV file."""
    if not data:
        return
    _ensure_dir(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys(), delimiter=delimiter)
        writer.writeheader()
        writer.writerows(data)
    log.info(f"[file] Wrote CSV: {path}")


def read_parquet(path: str | Path) -> pd.DataFrame:
    """Read a Parquet file into a DataFrame."""
    return pd.read_parquet(path)


def write_parquet(path: str | Path, df: pd.DataFrame, **kwargs) -> None:
    """Write a DataFrame to a Parquet file."""
    _ensure_dir(path)
    df.to_parquet(path, index=False, **kwargs)
    log.info(f"[file] Wrote Parquet: {path}")


def _ensure_dir(path: str | Path) -> None:
    """Create parent directories if they don't exist."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
