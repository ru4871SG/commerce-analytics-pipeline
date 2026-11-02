from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import BinaryIO, Protocol

from crm_lakehouse.contract import Table


class LandingFiles(Protocol):
    def list(self, folder: str) -> list[tuple[str, int]]:
        """Every file under folder, as (path, size in bytes)."""

    def open(self, path: str) -> BinaryIO:
        """The file's bytes, as stored (compressed)."""


class LocalFiles:
    """The landing volume as a local folder. Names starting with a dot or an underscore are the
    writers' temporary files, and are left out until they are renamed."""

    def list(self, folder: str) -> list[tuple[str, int]]:
        found = []
        for parent, directories, names in os.walk(folder):
            directories[:] = sorted(d for d in directories if not d.startswith((".", "_")))
            for name in sorted(names):
                if not name.startswith((".", "_")):
                    path = os.path.join(parent, name)
                    found.append((path, os.path.getsize(path)))
        return found

    def open(self, path: str) -> BinaryIO:
        return open(path, "rb")


@dataclass(frozen=True)
class Lakehouse:
    catalog: str                                    # Unity Catalog catalog: local, dev or prod
    root: str | None = None                         # local runs: the folder that plays Unity Catalog
    storage_root: str | None = None                 # Databricks: the external location of the shared tables
    credentials: Callable[[str, str], dict] | None = field(default=None, compare=False, repr=False)
    files: LandingFiles = field(default_factory=LocalFiles, compare=False, repr=False)

    def __post_init__(self):
        if (self.root is None) == (self.storage_root is None):
            raise ValueError("A lakehouse is either local (root) or on Databricks (storage_root), not both.")

    @property
    def is_local(self) -> bool:
        return self.root is not None

    def name(self, table: Table) -> str:
        """The Unity Catalog name, e.g. prod.silver.web_events."""
        return f"{self.catalog}.{table.schema}.{table.name}"

    def uri(self, table: Table) -> str:
        """Where the table's files are: a local folder, or its external location."""
        if self.is_local:
            return os.path.join(self.root, self.catalog, table.schema, table.name)
        return f"{self.storage_root.rstrip('/')}/{table.schema}/{table.name}"

    @property
    def landing(self) -> str:
        """The folder the storefronts' export files land in (a Unity Catalog volume)."""
        volume = f"Volumes/{self.catalog}/bronze/landing/web_events"
        return os.path.join(self.root, volume) if self.is_local else f"/{volume}"

    def storage_options(self, table: Table, operation: str = "READ") -> dict | None:
        """delta-rs storage options for the table: none locally, vended credentials otherwise.
        operation is READ or READ_WRITE."""
        if self.is_local:
            return None
        if self.credentials is None:
            raise RuntimeError(f"No credential provider to reach {self.name(table)} outside Databricks.")
        return self.credentials(self.name(table), operation)
