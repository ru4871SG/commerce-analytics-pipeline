"""Fixtures for the crm_lakehouse tests: a throwaway lakehouse, a local Spark session, and three
days of landing files with every edge case the engines must agree on (sample_days.py).

The tests import the package from the source tree. The Spark tests run where PySpark is
installed (.venv-spark: scripts/local.sh test) and are skipped elsewhere.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from crm_lakehouse.layout import Lakehouse  # noqa: E402


@pytest.fixture
def lakehouse(tmp_path) -> Lakehouse:
    return Lakehouse("test", root=str(tmp_path / "lakehouse"))


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from crm_lakehouse.spark_job import spark_session

    os.environ.setdefault("CRM_SPARK_IVY_DIR", str(REPO / ".local" / "ivy2"))
    os.environ.setdefault("CRM_SPARK_WAREHOUSE_DIR", str(REPO / ".local" / "spark-warehouse"))
    os.environ.setdefault("CRM_SPARK_MASTER", "local[2]")
    os.environ.setdefault("CRM_SPARK_SHUFFLE_PARTITIONS", "4")
    session = spark_session("crm_lakehouse tests")
    yield session
    session.stop()
