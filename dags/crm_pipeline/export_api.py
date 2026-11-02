"""Ask the CRM export service to refresh a brand's export snapshot before it is copied.
"""
from __future__ import annotations

import logging

import requests

from crm_pipeline import connections
from crm_pipeline.config import EXPORT_API_CONN_ID
from crm_pipeline.errors import PermanentError

log = logging.getLogger(__name__)


class ExportAlreadyRunning(Exception):
    """The export service is still busy with this brand (HTTP 429). Retrying later can succeed."""


def trigger_crm_export(brand_key: str, conn_id: str = EXPORT_API_CONN_ID, timeout: int = 900) -> dict:
    try:
        conn = connections.get_connection(conn_id)
    except connections.ConnectionNotFound:
        # The local demo has no export service: the source database is already current.
        # Any other error (a secrets backend that is down) fails the task, so a stale
        # snapshot is never loaded silently.
        log.info("No %s connection: no export service configured, using the database as it is.", conn_id)
        return {"skipped": True}
    extra = conn.extra
    url = extra["base_url"].rstrip("/") + extra.get("endpoint", "/api/export-crm")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        extra["secret_header"]: conn.password,
    }

    response = requests.post(url, headers=headers, data={"brand": brand_key}, timeout=timeout)
    log.info("Export of %s answered %s", brand_key, response.status_code)

    if response.status_code == 429:
        raise ExportAlreadyRunning(f"An export of {brand_key} is already running: {response.text[:200]}")
    if 400 <= response.status_code < 500:
        # Wrong secret, unknown brand, bad request: retrying will not help
        raise PermanentError(f"Export of {brand_key} was refused ({response.status_code}): {response.text[:500]}")
    response.raise_for_status()

    try:
        body = response.json()
    except ValueError:
        body = {"text": response.text[:500]}
    log.info("Export of %s finished: %s", brand_key, body)
    return body
