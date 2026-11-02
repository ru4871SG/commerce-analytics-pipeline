"""
CRM executive report pipeline
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, Param, TaskGroup, TriggerRule, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from crm_pipeline import bronze_loader, export_api, lakehouse, source_postgres, stream_task
from crm_pipeline.assets import ACTIVITY, GOLD
from crm_pipeline.config import INCREMENTAL_TABLES, SIMULATE_ACTIVITY, SOURCE_TABLES, TIMEZONE, brands_to_extract
from crm_pipeline.dbt import dbt_task
from crm_pipeline.notifications import notify_failure


@task(retries=4, retry_delay=timedelta(minutes=3))
def prepare(brand_key: str, database: str) -> dict:
    """Refresh the brand's export snapshot, then check it has every table the models need.
    HTTP 429 (an export is already running) is retried; a missing table fails at once."""
    exported = export_api.trigger_crm_export(brand_key)
    source_postgres.check_source_tables(database, SOURCE_TABLES)
    return exported


@task(execution_timeout=timedelta(hours=2))
def load_tables(brand_key: str, database: str) -> dict[str, int]:
    """Copy the brand's nine tables to RAW.CRM, read in one snapshot: the reference tables
    replace the brand's rows, the incremental ones append their changed rows to
    RAW.CRM.<TABLE>_CHANGES. Returns the rows copied per table."""
    with bronze_loader.snowflake_session() as snowflake:
        return bronze_loader.load_brand(snowflake, brand_key, database, SOURCE_TABLES,
                                        load_method=bronze_loader.load_method())


@task(trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(minutes=30))
def merge_task(table: str) -> dict[str, int]:
    """SELECT ... FROM <TABLE>_CHANGES_STREAM, then one run of the MERGE_<TABLE> task. The log
    shows which rows changed since the stream was last consumed, that is since the previous
    successful run (reading does not consume the stream). The task is skipped when the stream has
    no data: there is nothing to merge."""
    with bronze_loader.snowflake_session() as snowflake:
        cursor = snowflake.cursor()
        changes = stream_task.read_stream(cursor, table, stream_task.ensure_metadata(cursor))
        stream_task.log_stream(changes)
        run = stream_task.run_merge_task(cursor, table, get_current_context()["run_id"], "airflow", changes)
    if run.state == "SKIPPED":
        raise AirflowSkipException(f"{run.name}: {stream_task.stream_name(table)} has no data.")
    return {"inserted": run.inserted, "updated": run.updated}


@task(trigger_rule=TriggerRule.NONE_FAILED, execution_timeout=timedelta(minutes=30))
def reconcile_bronze() -> dict[str, int]:
    """Compare each merged table with the source figures its load recorded, per brand: a row
    count and an updated_at checksum. A difference means rows were deleted in the source or a
    change was never copied, and fails the task without a retry, so dbt never publishes it. With
    the resync_bronze parameter, the tables that differ are copied whole instead
    (crm_pipeline/bronze_loader.py, reconcile)."""
    resync = get_current_context()["params"]["resync_bronze"]
    with bronze_loader.snowflake_session() as snowflake:
        results = bronze_loader.reconcile(snowflake, brands_to_extract(), resync=resync,
                                          load_method=bronze_loader.load_method())
    differences = [result for result in results if not result.matches]
    if differences:
        raise AirflowFailException(bronze_loader.mismatch_message(
            differences, 'trigger crm_executive_report with {"resync_bronze": true}'))
    return {f"{result.brand}.{result.table}": result.bronze_rows for result in results}


@task(execution_timeout=timedelta(minutes=15))
def plan_web_batch() -> dict:
    """The next clickstream batch and the engine the gate chose for it, recorded in the batch
    ledger before anything runs (crm_lakehouse.planning)."""
    context = get_current_context()
    engine = context["params"]["web_engine"]
    return lakehouse.plan(None if engine == "auto" else engine, "airflow", context["run_id"])


@task.branch
def choose_web_engine(plan: dict) -> str:
    """Follow the gate: the in-process engine, Spark, or straight to publishing."""
    return f"lakehouse.{lakehouse.next_step(plan)}"


@task(execution_timeout=timedelta(hours=1))
def run_in_process(plan: dict) -> dict:
    """The batch with pandas and delta-rs, in this worker. A retry runs it on Spark instead."""
    context = get_current_context()
    return lakehouse.run_in_process(plan, "airflow", context["run_id"], context["ti"].try_number)


@task(execution_timeout=timedelta(hours=4))
def run_on_spark(plan: dict) -> dict:
    """The batch on Spark: the bundle's Databricks job, or Spark in .venv-spark locally."""
    return lakehouse.run_on_spark(plan, "airflow", get_current_context()["run_id"])


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, execution_timeout=timedelta(minutes=30))
def publish_web_funnel(plan: dict) -> dict:
    """Copy the batch's gold days into RAW.WEB, for dbt. Nothing to do when nothing was planned."""
    with bronze_loader.snowflake_session() as snowflake:
        return lakehouse.publish(plan, snowflake)


@dag(
    dag_id="crm_executive_report",
    schedule=[ACTIVITY] if SIMULATE_ACTIVITY else CronTriggerTimetable("30 7 * * *", timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,      # never two loads of the same brand at once; the streams rely on it
    max_active_tasks=4,     # at most a few brands extracting in parallel
    default_args={
        "owner": "Ruddy Gunawan",
        "retries": 2,
        "retry_delay": timedelta(minutes=4),
        "on_failure_callback": notify_failure,
    },
    params={
        "resync_bronze": Param(
            False, type="boolean",
            description="Copy whole every incremental table that differs from the source, instead of "
                        "failing reconcile_bronze. For rows really deleted in the source, or changes never copied.",
        ),
        "web_engine": Param(
            "auto", enum=["auto", "pandas", "spark"],
            description="The engine for the clickstream batch: auto lets the gate choose from the batch's size.",
        ),
    },
    tags=["crm", "snowflake", "dbt", "lakehouse", "executive-report"],
    doc_md=__doc__,
)
def crm_executive_report():
    loads = []
    for brand in brands_to_extract():
        with TaskGroup(group_id=f"ingest_{brand.key}", tooltip=f"{brand.name}: {brand.source_database}"):
            loaded = load_tables(brand.key, brand.source_database)
            prepare(brand.key, brand.source_database) >> loaded
        loads.append(loaded)

    # Freshness is only checked when this DAG loads bronze: brands that are not extracted (dummy
    # data) do not get fresher with time.
    build = dbt_task("dbt_build", check_source_freshness=bool(loads), outlets=[GOLD])

    with TaskGroup(group_id="lakehouse",
                   tooltip="Storefront clickstream: the gate picks pandas or Spark for each batch (crm_lakehouse)"):
        plan = plan_web_batch()
        engine = choose_web_engine(plan)
        in_process, on_spark, published = run_in_process(plan), run_on_spark(plan), publish_web_funnel(plan)
        engine >> [in_process, on_spark, published]
        [in_process, on_spark] >> published
    # dbt does not wait for the publish: a failed batch fails the run, not the report. dbt reads
    # whatever RAW.WEB holds; a day published after it did reaches gold with the next run.

    if loads:
        with TaskGroup(group_id="streams_and_tasks",
                       tooltip="Stand-in for Snowflake streams and tasks (crm_pipeline/stream_task.py)") as streams:
            merges = {
                table.name: merge_task.override(task_id=f"merge_{table.name}")(table.name)
                for table in INCREMENTAL_TABLES
            }
            for table in INCREMENTAL_TABLES:
                for parent in table.merge_after:
                    merges[parent] >> merges[table.name]    # the rows a foreign key points at go first
        loads >> streams
        streams >> reconcile_bronze() >> build


crm_executive_report()
