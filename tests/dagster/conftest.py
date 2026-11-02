"""Dagster tests run in .venv-dagster (no Airflow); they skip when Dagster is absent."""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

try:
    import dagster
except ImportError:
    dagster = None

if dagster is None or not hasattr(dagster, "AssetKey"):
    # In .venv there is no Dagster; tests/dagster itself is a namespace package called dagster,
    # so importorskip() alone would not skip this directory.
    pytest.skip("Dagster is not installed in this environment", allow_module_level=True)

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "dagster"))
sys.path.insert(0, str(REPO_ROOT / "dags"))


def _load_env() -> None:
    for path in (REPO_ROOT / ".env", REPO_ROOT / ".env.local.example"):
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            os.environ.setdefault(key.strip(), value)
        break


_load_env()
os.environ.setdefault("CRM_SIMULATE_ACTIVITY", "true")
os.environ.setdefault("DAGSTER_HOME", str(REPO_ROOT / ".dagster"))
os.environ.setdefault("DBT_PROFILES_DIR", str(REPO_ROOT / "dbt"))
os.environ.setdefault("DBT_BIN", str(REPO_ROOT / ".venv-dbt" / "bin" / "dbt"))

MANIFEST = REPO_ROOT / "dbt" / "target" / "dagster" / "manifest.json"


@pytest.fixture(scope="session", autouse=True)
def dbt_manifest():
    """dagster-dbt needs manifest.json when the definitions load."""
    if MANIFEST.exists():
        return MANIFEST
    dbt = Path(os.environ["DBT_BIN"])
    if not dbt.exists():
        pytest.skip("dbt is not installed; run scripts/local.sh install")
    subprocess.run(
        [str(dbt), "parse", "--target-path", "target/dagster"],
        cwd=REPO_ROOT / "dbt",
        env={**os.environ, "DBT_TARGET": "emulator"},
        check=True,
    )
    return MANIFEST
