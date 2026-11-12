"""Jobs, schedules, sensors and failure alerts.

Schedules and sensors start stopped, as Airflow's DAGs start paused. In the local demo the CLI
runs the side loads, clickstream, report and activation jobs itself because no daemon is running; with
`dagster dev` the sensors chain the jobs as the Airflow assets do.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dagster import (
    AssetSelection,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    define_asset_job,
    multiprocess_executor,
    run_failure_sensor,
    run_status_sensor,
)

from crm_pipeline.config import REPO_ROOT, SIMULATE_ACTIVITY, TIMEZONE
from crm_pipeline.notifications import failure_text, post_to_slack

from dagster_crm.activity import crm_daily_activity_job
from dagster_crm.activation import crm_executive_activation_job
from dagster_crm.side_loads import SIDE_LOADS

REPORT_TAGS = {"crm/report": "1", "dagster/max_runtime": "21600"}
CLICKSTREAM_TAGS = {"crm/clickstream": "1", "dagster/max_runtime": "21600"}
SIDE_LOADS_TAGS = {"crm/side_loads": "1", "dagster/max_runtime": "3600"}
REPORT_JOB_NAME = "crm_executive_report"
CLICKSTREAM_JOB_NAME = "crm_clickstream"
SIDE_LOADS_JOB_NAME = SIDE_LOADS
ACTIVATION_JOB_NAME = "crm_executive_activation"
LAST_ORCHESTRATOR = Path(REPO_ROOT) / ".local" / "last_orchestrator"


def write_last_orchestrator(name: str) -> None:
    LAST_ORCHESTRATOR.parent.mkdir(parents=True, exist_ok=True)
    LAST_ORCHESTRATOR.write_text(f"{name} {datetime.now(timezone.utc).isoformat()}\n")


crm_executive_report_job = define_asset_job(
    REPORT_JOB_NAME,
    selection=AssetSelection.groups("bronze", "dbt"),
    tags=REPORT_TAGS,
    # Each step runs in its own process. A forkserver that has imported the definitions once
    # forks them ready-made; spawning a fresh interpreter per step re-imports dagster,
    # dagster-dbt and the dbt manifest every time (about 2 s each, on 22 steps).
    executor_def=multiprocess_executor.configured({
        "max_concurrent": 4,
        "start_method": {"forkserver": {"preload_modules": ["dagster_crm.definitions"]}},
    }),
    description="Ingest every brand, merge the streams, reconcile; then dbt silver and gold.",
)

# The clickstream is a job of its own, as Airflow's lakehouse tasks are a branch dbt_build does
# not wait for: a failed batch fails this job, never the report. dbt reads whatever RAW.WEB
# holds; a day published after it did reaches gold with the next report.
crm_clickstream_job = define_asset_job(
    CLICKSTREAM_JOB_NAME,
    selection=AssetSelection.groups("lakehouse"),
    tags=CLICKSTREAM_TAGS,
    description="Process the waiting clickstream files on the engine the gate picks; publish the funnel "
                "into RAW.WEB for dbt.",
)


# The side loads are a job of their own for the same reason: one that fails, fails this job,
# never the report. The report's dbt step creates their tables first and reads whatever they
# hold (crm_pipeline/raw_tables.py).
crm_side_loads_job = define_asset_job(
    SIDE_LOADS_JOB_NAME,
    selection=AssetSelection.groups(SIDE_LOADS),
    tags=SIDE_LOADS_TAGS,
    description="Loads that feed dbt and that the report does not wait for: the ECB's exchange rates "
                "into RAW.REF, and the changes of orders from the write-ahead log into RAW.CRM.ORDERS_CDC.",
)


crm_executive_report_schedule = ScheduleDefinition(
    name="crm_executive_report_schedule",
    job=crm_executive_report_job,
    cron_schedule="30 7 * * *",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run the report at 07:30 New York when the simulator is not driving it.",
)

crm_clickstream_schedule = ScheduleDefinition(
    name="crm_clickstream_schedule",
    job=crm_clickstream_job,
    cron_schedule="30 7 * * *",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Process the clickstream at 07:30 New York, beside the report, when the simulator is not driving it.",
)

crm_side_loads_schedule = ScheduleDefinition(
    name="crm_side_loads_schedule",
    job=crm_side_loads_job,
    cron_schedule="30 7 * * *",
    execution_timezone=TIMEZONE,
    default_status=DefaultScheduleStatus.STOPPED,
    description="Run the side loads at 07:30 New York, beside the report, when the simulator is not driving it.",
)


@run_status_sensor(
    name="start_report_after_activity",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[crm_daily_activity_job],
    request_jobs=[crm_side_loads_job, crm_clickstream_job, crm_executive_report_job],
    default_status=DefaultSensorStatus.STOPPED,
)
def start_report_after_activity(context):
    run = context.dagster_run
    if run.job_name != "crm_daily_activity":
        return SkipReason(f"{run.job_name} is not the activity job")
    # The day's clickstream export arrives with its CRM activity, and the day has a new exchange
    # rate: the three jobs start, side by side
    return [
        RunRequest(run_key=f"{run.run_id}:side_loads", job_name=SIDE_LOADS_JOB_NAME, tags=SIDE_LOADS_TAGS),
        RunRequest(run_key=f"{run.run_id}:clickstream", job_name=CLICKSTREAM_JOB_NAME, tags=CLICKSTREAM_TAGS),
        RunRequest(run_key=run.run_id, job_name=REPORT_JOB_NAME, tags=REPORT_TAGS),
    ]


@run_status_sensor(
    name="start_activation_after_report",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[crm_executive_report_job],
    request_job=crm_executive_activation_job,
    default_status=DefaultSensorStatus.STOPPED,
)
def start_activation_after_report(context):
    run = context.dagster_run
    if run.job_name != REPORT_JOB_NAME:
        return SkipReason(f"{run.job_name} is not the report job")
    # The dashboard is showing this orchestrator's gold.
    write_last_orchestrator("dagster")
    return RunRequest(run_key=run.run_id, job_name=ACTIVATION_JOB_NAME)


@run_failure_sensor(
    name="crm_failure_alert",
    default_status=DefaultSensorStatus.STOPPED,
)
def crm_failure_alert(context):
    run = context.dagster_run
    message = getattr(context.failure_event, "message", str(context.failure_event))
    post_to_slack(failure_text(run.job_name, run.run_id, message))


def jobs() -> list:
    return [crm_executive_report_job, crm_clickstream_job, crm_side_loads_job, crm_executive_activation_job]


def schedules() -> list:
    return [] if SIMULATE_ACTIVITY else [crm_executive_report_schedule, crm_clickstream_schedule,
                                         crm_side_loads_schedule]


def sensors() -> list:
    result = [start_report_after_activity, start_activation_after_report, crm_failure_alert]
    return result if SIMULATE_ACTIVITY else [start_activation_after_report, crm_failure_alert]
