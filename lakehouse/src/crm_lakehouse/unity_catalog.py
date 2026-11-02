"""Reaching the Databricks lakehouse from outside Databricks: the orchestrator's side.
"""
from __future__ import annotations

import io
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import BinaryIO

# Credentials are renewed this long before they expire, so a write never starts on a token
# about to lapse
RENEW_BEFORE = timedelta(minutes=10)


def vended_credentials(workspace, region: str | None = None) -> Callable[[str, str], dict]:
    """A credential provider for layout.Lakehouse: delta-rs storage options for a table, from
    Unity Catalog's temporary table credentials. Cached per table and operation until shortly
    before they expire."""
    from databricks.sdk.service.catalog import TableOperation

    cache: dict[tuple[str, str], tuple[datetime, dict]] = {}

    def credentials(full_name: str, operation: str) -> dict:
        key = (full_name, operation)
        cached = cache.get(key)
        if cached and cached[0] - RENEW_BEFORE > datetime.now(timezone.utc):
            return cached[1]
        table_id = workspace.tables.get(full_name).table_id
        vended = workspace.temporary_table_credentials.generate_temporary_table_credentials(
            table_id=table_id, operation=TableOperation(operation))
        aws = vended.aws_temp_credentials
        if aws is None:
            raise RuntimeError(f"Unity Catalog vended no AWS credentials for {full_name}.")
        options = {
            "AWS_ACCESS_KEY_ID": aws.access_key_id,
            "AWS_SECRET_ACCESS_KEY": aws.secret_access_key,
            "AWS_SESSION_TOKEN": aws.session_token,
            # S3 conditional writes make a commit fail rather than overwrite a concurrent one
            "conditional_put": "etag",
        }
        if region:
            options["AWS_REGION"] = region
        expires = datetime.fromtimestamp(vended.expiration_time / 1000, tz=timezone.utc)
        cache[key] = (expires, options)
        return options

    return credentials


class VolumeFiles:
    """layout.LandingFiles over the Files API: the landing volume as Databricks stores it."""

    def __init__(self, workspace):
        self.workspace = workspace

    def list(self, folder: str) -> list[tuple[str, int]]:
        found, pending = [], [folder]
        while pending:
            for entry in self.workspace.files.list_directory_contents(pending.pop()):
                name = entry.path.rstrip("/").rsplit("/", 1)[-1]
                if name.startswith((".", "_")):
                    continue
                if entry.is_directory:
                    pending.append(entry.path)
                else:
                    found.append((entry.path, entry.file_size or 0))
        return sorted(found)

    def open(self, path: str) -> BinaryIO:
        # A day's file is a few megabytes: read it whole
        return io.BytesIO(self.workspace.files.download(path).contents.read())
