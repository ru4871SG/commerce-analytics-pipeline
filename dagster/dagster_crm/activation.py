"""What happens after gold: the KPI digest, then the Superset dashboard check.

This job is started by a run status sensor on a successful report run, so the report job does
not wait for Slack or Superset, as in Airflow. The dashboard check reports state = SKIPPED when
the script exits 99 (Superset is not running).
"""
from __future__ import annotations

import subprocess
import sys

from dagster import AssetKey, Failure, MaterializeResult, asset, define_asset_job

from crm_pipeline import digest
from crm_pipeline.config import REPO_ROOT, SUPERSET_NOT_RUNNING
from crm_pipeline.notifications import post_to_slack

from dagster_crm.resources import SnowflakeWarehouse
from dagster_crm.retries import DEFAULT_RETRY, NO_RETRY


@asset(
    key="executive_digest",
    group_name="activation",
    deps=[AssetKey("exec_kpi_snapshot"), AssetKey("exec_channel_cac_ltv"), AssetKey("exec_kpi_anomalies")],
    retry_policy=DEFAULT_RETRY,
    description="Read the KPI cards, channel CAC/LTV and the as-of day's unusual numbers from gold; "
                "post the digest to Slack.",
)
def executive_digest(context, warehouse: SnowflakeWarehouse) -> MaterializeResult:
    with warehouse.session() as connection:
        gold = digest.read_gold(connection.cursor())
    executive = digest.build_digest(gold)
    published = post_to_slack(digest.digest_message(executive))
    context.log.info("Executive digest for %s: %s unusual number(s), %s alert(s), %s.", executive["as_of_date"],
                     len(executive["anomalies"]), len(executive["alerts"]),
                     "posted to Slack" if published else "logged only")
    return MaterializeResult(
        metadata={
            "as_of_date": executive["as_of_date"] or "",
            "anomalies": len(executive["anomalies"]),
            "alerts": len(executive["alerts"]),
            "posted_to_slack": published,
        }
    )


@asset(
    key="verify_superset_dashboard",
    group_name="activation",
    deps=[AssetKey("executive_digest")],
    retry_policy=NO_RETRY,
    description="Query every dashboard chart through Superset's API; skipped when Superset is down.",
)
def verify_superset_dashboard(context) -> MaterializeResult:
    process = subprocess.run(
        [sys.executable, "local/superset/check_dashboard.py"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if process.stdout:
        context.log.info(process.stdout.rstrip())
    if process.stderr:
        context.log.warning(process.stderr.rstrip())
    if process.returncode == SUPERSET_NOT_RUNNING:
        context.log.info("Superset is not running: dashboard check skipped.")
        return MaterializeResult(metadata={"state": "SKIPPED"})
    if process.returncode != 0:
        raise Failure(
            f"Superset dashboard check failed with exit code {process.returncode}.",
            allow_retries=False,
        )
    return MaterializeResult(metadata={"state": "PASSED"})


crm_executive_activation_job = define_asset_job(
    "crm_executive_activation",
    selection=[executive_digest, verify_superset_dashboard],
    description="Post the executive digest and check the Superset dashboard.",
)

ACTIVATION_ASSETS = [executive_digest, verify_superset_dashboard]
