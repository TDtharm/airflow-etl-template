from __future__ import annotations

import csv
import io
import json
from typing import Any

import pandas as pd


def to_dataframe(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Convert list[dict] query result to pandas DataFrame."""
    return pd.DataFrame(rows)


def to_json(rows: list[dict[str, Any]], **kwargs) -> str:
    """Convert list[dict] query result to JSON string."""
    return json.dumps(rows, ensure_ascii=False, default=str, **kwargs)


def to_csv(rows: list[dict[str, Any]], delimiter: str = ",") -> str:
    """Convert list[dict] query result to CSV string."""
    if not rows:
        return ""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=rows[0].keys(), delimiter=delimiter)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def from_dataframe(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert pandas DataFrame back to list[dict]."""
    return df.to_dict(orient="records")


def from_json(text: str) -> list[dict[str, Any]]:
    """Parse JSON string to list[dict]."""
    return json.loads(text)
