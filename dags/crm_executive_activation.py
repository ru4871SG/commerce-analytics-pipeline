"""
Executive report activation
"""
from __future__ import annotations

import shlex
import sys
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from crm_pipeline import bronze_loader, digest
from crm_pipeline.assets import GOLD
from crm_pipeline.config import REPO_ROOT, SUPERSET_NOT_RUNNING
from crm_pipeline.notifications import notify_failure, post_to_slack


@task(execution_timeout=timedelta(minutes=10))
def build_executive_digest() -> dict:
    """The headline numbers per brand, and a sentence for each one past its alert threshold."""
    with bronze_loader.snowflake_session() as snowflake:
        gold = digest.read_gold(snowflake.cursor())
    return digest.build_digest(gold)


@task(execution_timeout=timedelta(minutes=10))
def publish_executive_digest(executive_digest: dict) -> bool:
    """Post the digest to Slack. Returns whether it went to Slack rather than only to the log."""
    return post_to_slack(digest.digest_message(executive_digest))


@dag(
    dag_id="crm_executive_activation",
    schedule=[GOLD],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "Ruddy Gunawan",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["crm", "executive-report", "activation"],
    doc_md=__doc__,
)
def crm_executive_activation():
    publish_executive_digest(build_executive_digest())
    BashOperator(
        task_id="verify_superset_dashboard",
        # The worker's own interpreter, as the Dagster job uses: it has requests and PyYAML, which
        # the first python3 on the PATH may not
        bash_command=f"{shlex.quote(sys.executable)} local/superset/check_dashboard.py",
        cwd=str(REPO_ROOT),
        skip_on_exit_code=SUPERSET_NOT_RUNNING,
        retries=0,              # a chart that fails its query fails again; fix the bundle instead
        execution_timeout=timedelta(minutes=10),
    )


crm_executive_activation()
