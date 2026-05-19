"""Google Chat webhook notification for Airflow callbacks."""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone


GCHAT_WEBHOOK_URL = os.getenv("GCHAT_WEBHOOK_URL", "")


def _send_gchat(text: str) -> None:
    """Send a message to Google Chat via webhook."""
    if not GCHAT_WEBHOOK_URL:
        return

    payload = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        GCHAT_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def on_failure_callback(context: dict) -> None:
    """Airflow callback — notify Google Chat on task failure."""
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("execution_date", "")
    exception = context.get("exception", "")
    log_url = task_instance.log_url if task_instance else ""

    text = (
        f"🔴 *Task Failed*\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task_id}`\n"
        f"Execution: {execution_date}\n"
        f"Error: {exception}\n"
        f"Log: {log_url}"
    )
    _send_gchat(text)


def on_retry_callback(context: dict) -> None:
    """Airflow callback — notify Google Chat on task retry."""
    task_instance = context.get("task_instance")
    dag_id = task_instance.dag_id if task_instance else "unknown"
    task_id = task_instance.task_id if task_instance else "unknown"
    try_number = task_instance.try_number if task_instance else 0
    exception = context.get("exception", "")

    text = (
        f"🟡 *Task Retry*\n"
        f"DAG: `{dag_id}`\n"
        f"Task: `{task_id}`\n"
        f"Attempt: {try_number}\n"
        f"Error: {exception}"
    )
    _send_gchat(text)


def on_success_callback(context: dict) -> None:
    """Airflow callback — notify Google Chat on DAG success (use as dag-level callback)."""
    dag_run = context.get("dag_run")
    dag_id = dag_run.dag_id if dag_run else "unknown"
    execution_date = context.get("execution_date", "")

    text = (
        f"🟢 *DAG Success*\n"
        f"DAG: `{dag_id}`\n"
        f"Execution: {execution_date}"
    )
    _send_gchat(text)
