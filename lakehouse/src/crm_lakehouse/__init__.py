"""The storefront clickstream on the lakehouse: one data contract, two engines, and the gate that
picks one for each batch."""
from __future__ import annotations

import hashlib
from importlib import resources

__version__ = "0.1.2"


def contract_fingerprint() -> str:
    """A hash of this package's source. The orchestrator writes it into each batch it plans and
    the Spark job compares it with its own, so the two engines never write the same tables with
    different releases of the code: an Airflow worker image and a Databricks bundle are deployed
    separately, and can drift. Computed from the files themselves, so a wheel and the source tree
    it was built from agree."""
    digest = hashlib.sha256()
    package = resources.files(__name__)
    for name in sorted(entry.name for entry in package.iterdir() if entry.name.endswith(".py")):
        digest.update(name.encode())
        digest.update(package.joinpath(name).read_bytes())
    return digest.hexdigest()[:16]
