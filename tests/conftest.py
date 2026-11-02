"""Make the DAG helpers importable the way Airflow does: Airflow puts the dags folder on sys.path."""
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))
os.environ.setdefault("AIRFLOW_HOME", str(REPO_ROOT / ".airflow"))
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
# Demo mode, so the activity simulator DAG is parsed too
os.environ.setdefault("CRM_SIMULATE_ACTIVITY", "true")


@pytest.fixture(scope="session")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(REPO_ROOT / "dags"))


@pytest.fixture(scope="session")
def dag(dagbag):
    return dagbag.dags["crm_executive_report"]   # get_dag() would query the metadata database
