"""The Dagster definitions: graph shape, dependencies, partitions and automation."""
from __future__ import annotations

import json
from contextlib import nullcontext
from datetime import datetime

import pytest
from dagster import (
    AssetKey,
    BackfillPolicy,
    DagsterRunStatus,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Failure,
    materialize,
)

from crm_pipeline import bronze_loader
from crm_pipeline.config import INCREMENTAL_TABLES, REFERENCE_TABLES, brands_to_extract
from crm_pipeline.errors import PermanentError
from crm_pipeline.source_postgres import TableFigures
from crm_pipeline.stream_task import changes_table

from dagster_crm import activity, automation, bronze, dbt_assets, lakehouse, side_loads
from dagster_crm.definitions import defs
from dagster_crm.resources import SnowflakeWarehouse
from dagster_crm.retries import call


JOBS = ("crm_daily_activity", "crm_executive_report", "crm_clickstream", "crm_side_loads", "crm_executive_activation")


def test_definitions_load_with_every_resource():
    repo = defs.get_repository_def()
    assert set(JOBS) <= set(repo.job_names)
    for name in JOBS:
        job = repo.get_job(name)
        assert {"warehouse", "dbt"} <= set(job.resource_defs), name


def test_one_prepare_and_one_load_per_extracted_brand():
    names = set(bronze.bronze_ingest.node_def.node_names())
    assert names == {f"{step}_{brand.key}" for brand in brands_to_extract() for step in ("prepare", "load")} | {
        "collect_loads"}
    for brand in brands_to_extract():
        # The load reads the prepare op's output, and the brand's nine tables in one snapshot
        assert bronze.bronze_ingest.node_def.node_dict[f"load_{brand.key}"].input_dict["prepared"]


def test_merges_follow_the_foreign_keys_and_need_only_changes():
    by_name = {spec.key.path[-1]: spec for spec in bronze.bronze_merge.specs}
    assert set(by_name) == {table.name for table in INCREMENTAL_TABLES}
    for table in INCREMENTAL_TABLES:
        deps = {dep.asset_key for dep in by_name[table.name].deps}
        expected = {AssetKey(["raw", "crm", changes_table(table.name)])} | {
            AssetKey(["raw", "crm", parent]) for parent in table.merge_after
        }
        assert deps == expected, table.name
        # No reference table is a merge dependency.
        assert not deps & {AssetKey(["raw", "crm", table.name]) for table in REFERENCE_TABLES}


def test_the_merge_step_runs_parents_before_children():
    order = [table.name for table in bronze.MERGE_ORDER]
    assert sorted(order) == sorted(table.name for table in INCREMENTAL_TABLES)
    for table in INCREMENTAL_TABLES:
        for parent in table.merge_after:
            assert order.index(parent) < order.index(table.name), table.name


def test_the_report_job_merges_and_reconciles_in_one_step():
    job = defs.get_repository_def().get_job("crm_executive_report")
    assert set(job.graph.node_names()) == {"bronze_ingest", "bronze_merge", "crm_dbt_assets"}


def test_the_clickstream_is_a_job_of_its_own_so_a_failed_batch_never_stops_the_report():
    # The report's dbt step reads raw/web without waiting for web_events, as Airflow's dbt_build
    # does not wait for publish_web_funnel
    job = defs.get_repository_def().get_job("crm_clickstream")
    assert set(job.graph.node_names()) == {"web_events"}
    assert job.tags["crm/clickstream"] == "1"
    assert job.tags["dagster/max_runtime"] == "21600"
    # One run at a time of each job, as the instance's run queue enforces
    import yaml

    limits = yaml.safe_load((dbt_assets.REPO_ROOT / "dagster" / "dagster.yaml").read_text())
    assert {(limit["key"], limit["value"], limit["limit"]) for limit in limits["run_queue"]["tag_concurrency_limits"]} == {
        ("crm/report", "1", 1), ("crm/clickstream", "1", 1), ("crm/side_loads", "1", 1)}


def test_the_side_loads_are_a_job_of_their_own_so_a_failed_load_never_stops_the_report():
    # Every dbt model is one step: an asset upstream of it inside the report job would stop all of
    # gold. The report's dbt step reads raw/ref without waiting for the load, as Airflow's
    # dbt_build does not wait for load_fx_rates
    job = defs.get_repository_def().get_job("crm_side_loads")
    assert set(job.graph.node_names()) == {"load_fx_rates", "capture_orders_cdc"}
    assert job.tags == automation.SIDE_LOADS_TAGS == {"crm/side_loads": "1", "dagster/max_runtime": "3600"}
    assert side_loads.load_fx_rates.op.tags.get("crm/side_loads") == "1"
    assert side_loads.load_fx_rates.group_names_by_key == {AssetKey(["raw", "ref", "fx_rates"]): "crm_side_loads"}
    # The change data capture of orders writes a table of RAW.CRM, and is still no part of the
    # report job: the loader's tables are that job's, this one is a side load's
    assert side_loads.capture_orders_cdc.op.tags.get("crm/side_loads") == "1"
    assert side_loads.capture_orders_cdc.group_names_by_key == {AssetKey(["raw", "crm", "orders_cdc"]): "crm_side_loads"}
    report = defs.get_repository_def().get_job("crm_executive_report")
    assert not {"load_fx_rates", "capture_orders_cdc"} & set(report.graph.node_names())


def test_a_day_of_activity_starts_the_side_loads_the_clickstream_and_the_report():
    from dagster import DagsterEvent, DagsterInstance, DagsterRun, build_run_status_sensor_context

    run = DagsterRun(job_name="crm_daily_activity", run_id="activity-run")
    event = DagsterEvent(event_type_value="PIPELINE_SUCCESS", job_name="crm_daily_activity")
    with DagsterInstance.ephemeral() as instance:
        context = build_run_status_sensor_context(sensor_name="start_report_after_activity", dagster_event=event,
                                                  dagster_instance=instance, dagster_run=run)
        requests = automation.start_report_after_activity(context)
    # Each with the tag its run queue limit counts
    assert {request.job_name: request.tags for request in requests} == {
        "crm_side_loads": automation.SIDE_LOADS_TAGS, "crm_clickstream": automation.CLICKSTREAM_TAGS,
        "crm_executive_report": automation.REPORT_TAGS}
    assert len({request.run_key for request in requests}) == 3


def test_every_dbt_source_maps_to_a_bronze_or_lakehouse_asset():
    manifest = json.loads(dbt_assets.dbt_project.manifest_path.read_text())
    # Each source is the asset of the step that loads it, in that step's group: the group is what
    # the jobs select, so a source in the wrong one would be loaded by the wrong job, or by none
    groups = {
        key: group
        for asset in [bronze.bronze_merge, bronze.bronze_ingest, lakehouse.web_events, side_loads.load_fx_rates,
                      side_loads.capture_orders_cdc]
        for key, group in asset.group_names_by_key.items()
    }
    sources = list(manifest["sources"].values())
    assert {source["source_name"] for source in sources} == {"crm", "web", "ref"}
    for source in sources:
        key = dbt_assets.dbt_translator.get_asset_key(source)
        assert key in groups, key
        assert dbt_assets.dbt_translator.get_group_name(source) == groups[key], key
    assert groups[AssetKey(["raw", "ref", "fx_rates"])] == "crm_side_loads"
    # One table of the crm source is not the loader's: the change data capture's
    assert groups[AssetKey(["raw", "crm", "orders_cdc"])] == "crm_side_loads"
    assert groups[AssetKey(["raw", "crm", "orders_changes"])] == "bronze"


def test_reconcile_checks_block_and_cover_the_five_merged_tables():
    specs = list(bronze.bronze_merge.check_specs)
    assert {spec.asset_key for spec in specs} == {AssetKey(["raw", "crm", table.name]) for table in INCREMENTAL_TABLES}
    assert all(spec.blocking for spec in specs)
    assert {spec.name for spec in specs} == {f"reconcile_{table.name}" for table in INCREMENTAL_TABLES}


def test_activity_partitions_start_on_the_first_business_day_and_backfill_in_one_run():
    partitions = activity.crm_source_daily_activity.partitions_def
    assert partitions.get_first_partition_key() == "2026-10-01"
    assert partitions.timezone == "America/New_York"
    assert activity.crm_source_daily_activity.backfill_policy == BackfillPolicy.single_run()


def test_activity_only_exists_in_demo_mode(monkeypatch):
    assert activity.assets() and activity.jobs() and activity.schedules()
    monkeypatch.setattr(activity, "SIMULATE_ACTIVITY", False)
    assert activity.assets() == []
    assert activity.jobs() == []
    assert activity.schedules() == []


def test_report_job_carries_the_concurrency_and_runtime_tags():
    repo = defs.get_repository_def()
    job = repo.get_job("crm_executive_report")
    assert job.tags["crm/report"] == "1"
    assert job.tags["dagster/max_runtime"] == "21600"


def test_the_report_schedule_exists_only_without_the_simulator(monkeypatch):
    assert automation.schedules() == []
    monkeypatch.setattr(automation, "SIMULATE_ACTIVITY", False)
    schedules = automation.schedules()
    assert [schedule.name for schedule in schedules] == ["crm_executive_report_schedule", "crm_clickstream_schedule",
                                                         "crm_side_loads_schedule"]
    for schedule in schedules:
        assert schedule.cron_schedule == "30 7 * * *"
        assert schedule.execution_timezone == "America/New_York"


def test_schedules_and_sensors_start_stopped():
    repo = defs.get_repository_def()
    assert all(schedule.default_status == DefaultScheduleStatus.STOPPED for schedule in repo.schedule_defs)
    assert all(sensor.default_status == DefaultSensorStatus.STOPPED for sensor in repo.sensor_defs)
    assert {sensor.name for sensor in repo.sensor_defs} == {
        "start_report_after_activity", "start_activation_after_report", "crm_failure_alert",
    }


class _StreamCursor:
    """The queries a merge asset sends when its stream is empty."""

    def __init__(self):
        self._rows = []
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append(sql.split()[0].lower())
        if "max(OFFSET_LOADED_AT)" in sql:
            self._rows = [(datetime(2026, 10, 22, 11, 2),)]
        elif "information_schema.columns" in sql:
            self._rows = [("_TASK_HISTORY", column) for column in ("NAME", "STATE", "RUN_ID", "ORCHESTRATOR")] + [
                ("_STREAM_OFFSETS", "STREAM_NAME")] + [
                (f"{table.upper()}_CHANGES", column) for table in ("customers", "orders", "order_items",
                                                                  "refund_payments", "marketing_spend")
                for column in ("ID", "STATE", "UPDATED_AT", "_BRAND", "_LOADED_AT")]
        else:                     # e.g. the stream's counts per brand: none
            self._rows = []

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows

    def close(self):
        pass


class _Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def close(self):
        pass


def matching_reconciliation(monkeypatch, differs=()):
    """reconcile answering for each brand and table, equal figures unless the table is in differs."""
    def reconcile(connection, brands, tables, resync, load_method):
        return [
            bronze_loader.Reconciliation(table.name, brand.key, TableFigures(10, 100),
                                         TableFigures(10, 90 if table.name in differs else 100))
            for brand in brands for table in tables
        ]

    monkeypatch.setattr(bronze.bronze_loader, "reconcile", reconcile)


def run_merge_step(monkeypatch, differs=()):
    cursor = _StreamCursor()
    monkeypatch.setattr(SnowflakeWarehouse, "session", lambda self: nullcontext(_Connection(cursor)))
    monkeypatch.setattr(SnowflakeWarehouse, "load_method", lambda self: "insert")
    matching_reconciliation(monkeypatch, differs)
    result = materialize([bronze.bronze_merge], resources={"warehouse": SnowflakeWarehouse()}, raise_on_error=False)
    return result, cursor


def test_merges_materialize_with_state_skipped_when_their_streams_are_empty(monkeypatch):
    result, cursor = run_merge_step(monkeypatch)
    assert result.success
    events = result.get_asset_materialization_events()
    assert len(events) == len(INCREMENTAL_TABLES)                   # every merge, the later ones too
    for event in events:
        metadata = event.materialization.metadata
        assert metadata["state"].text == "SKIPPED"
        assert metadata["stream_rows"].value == 0
    assert "merge" not in cursor.statements
    assert "create" not in cursor.statements             # the metadata tables are there already
    checks = result.get_asset_check_evaluations()
    assert {check.check_name for check in checks} == {f"reconcile_{t.name}" for t in INCREMENTAL_TABLES}
    assert all(check.passed for check in checks)


def test_a_difference_fails_the_merge_step_without_a_retry(monkeypatch):
    result, _ = run_merge_step(monkeypatch, differs={"orders"})
    assert not result.success
    checks = {check.check_name: check.passed for check in result.get_asset_check_evaluations()}
    assert checks["reconcile_orders"] is False
    assert checks["reconcile_customers"] is True
    failure = next(event for event in result.all_events if event.event_type_value == "STEP_FAILURE")
    assert "Bronze does not match the source" in failure.event_specific_data.error.message
    assert not [event for event in result.all_events if event.event_type_value == "STEP_UP_FOR_RETRY"]


class _Invocation:
    """What dbt.cli() returns for a command that is not run."""

    def __init__(self, successful: bool):
        self._successful = successful

    def wait(self):
        return self

    def is_successful(self):
        return self._successful

    def stream(self):
        return iter(())


def run_dbt_step(monkeypatch, freshness_passes=True):
    """The dbt step with dbt's process replaced: its result, and each dbt command line dagster-dbt built."""
    from dagster_dbt import DbtCliResource
    from dagster_dbt.core.dbt_cli_invocation import DbtCliInvocation

    commands = []

    def run(args, **kwargs):
        commands.append(list(args))
        return _Invocation(freshness_passes or "freshness" not in args)

    # The step first creates the side loads' tables, in the warehouse: recorded where dbt's commands are
    monkeypatch.setattr(dbt_assets.raw_tables, "ensure_side_tables", lambda: commands.append(["side tables"]))
    monkeypatch.setattr(DbtCliInvocation, "run", staticmethod(run))
    monkeypatch.setattr(DbtCliResource, "_initialize_dbt_core_adapter", lambda self, args: None)
    result = materialize([dbt_assets.crm_dbt_assets], resources={"dbt": dbt_assets.dbt_resource},
                         raise_on_error=False)
    return result, commands


def test_dbt_source_freshness_checks_every_source(monkeypatch):
    result, commands = run_dbt_step(monkeypatch)
    assert result.success
    # The side loads' tables come first: their job is not part of this one, and dbt reads them
    side_tables, freshness, build = commands
    assert side_tables == ["side tables"]
    # Given the asset context, dagster-dbt would add --select fqn:*, which matches no source
    assert freshness == [dbt_assets.DBT_BIN, "source", "freshness"]
    assert build[1] == "build"


def test_stale_bronze_fails_the_dbt_step_before_the_build(monkeypatch):
    result, commands = run_dbt_step(monkeypatch, freshness_passes=False)
    assert not result.success
    assert [command[-1] for command in commands] == ["side tables", "freshness"]      # dbt build never ran
    failure = next(event for event in result.all_events if event.event_type_value == "STEP_FAILURE")
    assert "dbt source freshness failed" in failure.event_specific_data.error.message
    assert not [event for event in result.all_events if event.event_type_value == "STEP_UP_FOR_RETRY"]


def test_permanent_error_becomes_a_non_retryable_failure():
    def fail():
        raise PermanentError("bad configuration")

    with pytest.raises(Failure) as caught:
        call(fail)
    assert caught.value.allow_retries is False
    assert "bad configuration" in caught.value.description


def test_failure_sensors_are_stopped_and_success_sensors_chain_the_jobs():
    repo = defs.get_repository_def()
    report_sensor = next(sensor for sensor in repo.sensor_defs if sensor.name == "start_report_after_activity")
    activation_sensor = next(sensor for sensor in repo.sensor_defs if sensor.name == "start_activation_after_report")
    assert report_sensor.default_status == DefaultSensorStatus.STOPPED
    assert activation_sensor.default_status == DefaultSensorStatus.STOPPED
    assert report_sensor._run_status == DagsterRunStatus.SUCCESS


def materialize_fx_load(monkeypatch, load):
    monkeypatch.setattr(side_loads.fx_rates, "load", load)
    return materialize([side_loads.load_fx_rates], resources={"warehouse": SnowflakeWarehouse()}, raise_on_error=False)


def test_the_fx_load_materializes_with_state_skipped_without_its_connection(monkeypatch):
    skipped = {"state": "SKIPPED", "reason": "No ecb_fx_api connection: no rates are asked for.", "added": 0}
    result = materialize_fx_load(monkeypatch, lambda open_session: skipped)
    assert result.success
    (event,) = result.get_asset_materialization_events()
    assert event.asset_key == AssetKey(["raw", "ref", "fx_rates"])
    assert event.materialization.metadata["state"].text == "SKIPPED"
    assert event.materialization.metadata["added"].value == 0


def test_the_fx_load_says_what_it_added(monkeypatch):
    sessions = []

    def load(open_session):
        sessions.append(open_session)
        return {"state": "LOADED", "requested_from": "2026-11-11", "requested_to": "2026-11-12", "received": 4,
                "added": 4, "latest_rate_date": "2026-11-12"}

    result = materialize_fx_load(monkeypatch, load)
    assert result.success
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert (metadata["state"].text, metadata["added"].value, metadata["latest_rate_date"].text) == (
        "LOADED", 4, "2026-11-12")
    assert sessions[0].__name__ == "session"            # the warehouse resource's session, opened by the load


def test_a_failed_fx_load_fails_its_job_without_a_retry(monkeypatch):
    def load(open_session):
        raise PermanentError("The ECB API did not answer in 3 tries: The ECB API answered 503")

    result = materialize_fx_load(monkeypatch, load)
    assert not result.success
    failure = next(event for event in result.all_events if event.event_type_value == "STEP_FAILURE")
    assert "did not answer in 3 tries" in failure.event_specific_data.error.message
    assert not [event for event in result.all_events if event.event_type_value == "STEP_UP_FOR_RETRY"]


def materialize_capture(monkeypatch, capture_all):
    monkeypatch.setattr(side_loads.cdc, "capture_all", capture_all)
    return materialize([side_loads.capture_orders_cdc], resources={"warehouse": SnowflakeWarehouse()},
                       raise_on_error=False)


def test_the_capture_materializes_with_state_skipped_without_its_connection(monkeypatch):
    skipped = {"state": "SKIPPED", "changes": 0, "inserts": 0, "updates": 0, "deletes": 0, "added": 0, "lag_bytes": 0,
               "brands": {brand.key: {"state": "SKIPPED", "reason": "No crm_source_cdc connection", "changes": 0}
                          for brand in side_loads.BRANDS}}
    result = materialize_capture(monkeypatch, lambda brands, open_session: dict(skipped))
    assert result.success
    (event,) = result.get_asset_materialization_events()
    assert event.asset_key == AssetKey(["raw", "crm", "orders_cdc"])
    metadata = event.materialization.metadata
    assert metadata["state"].text == "SKIPPED" and metadata["changes"].value == 0
    assert metadata["broomx"].text == "SKIPPED"


def test_the_capture_reads_every_extracted_brand_and_says_what_it_found(monkeypatch):
    seen = []

    def capture_all(brands, open_session):
        seen.append(([brand.key for brand in brands], open_session.__name__))
        return {"state": "CAPTURED", "changes": 7, "inserts": 4, "updates": 2, "deletes": 1, "added": 7, "lag_bytes": 56,
                "brands": {brand.key: {"state": "CAPTURED", "changes": 7 if brand.key == "broomx" else 0,
                                       "slot": f"cdc_{brand.source_database}", "slot_lsn": "0/404AC90"}
                           for brand in brands}}

    result = materialize_capture(monkeypatch, capture_all)
    assert result.success
    assert seen == [([brand.key for brand in brands_to_extract()], "session")]      # the warehouse resource's session
    metadata = result.get_asset_materialization_events()[0].materialization.metadata
    assert (metadata["state"].text, metadata["changes"].value, metadata["deletes"].value, metadata["lag_bytes"].value) == (
        "CAPTURED", 7, 1, 56)
    assert metadata["broomx"].text == "7 changes, slot cdc_crm_broomx at 0/404AC90"


def test_a_lost_slot_fails_the_side_loads_job_without_a_retry(monkeypatch):
    def capture_all(brands, open_session):
        raise PermanentError("The replication slot cdc_crm_wecare was invalidated (wal_removed)")

    result = materialize_capture(monkeypatch, capture_all)
    assert not result.success
    failure = next(event for event in result.all_events if event.event_type_value == "STEP_FAILURE")
    assert "cdc_crm_wecare was invalidated" in failure.event_specific_data.error.message
    assert not [event for event in result.all_events if event.event_type_value == "STEP_UP_FOR_RETRY"]


def test_the_clickstream_step_publishes_what_dbt_reads():
    keys = set(lakehouse.web_events.keys)
    assert {AssetKey(["raw", "web", "daily_funnel"]), AssetKey(["raw", "web", "event_batches"])} <= keys
    assert {AssetKey(["lakehouse", "gold", "web_daily_funnel"])} <= keys
    # It runs in the clickstream job, one at a time
    assert lakehouse.web_events.op.tags.get("crm/clickstream") == "1"


def test_the_digest_waits_for_every_gold_table_it_reads():
    from crm_pipeline import digest
    from dagster_crm.activation import executive_digest

    waits_for = {dep.asset_key for dep in executive_digest.specs_by_key[AssetKey("executive_digest")].deps}
    assert waits_for == {AssetKey("exec_kpi_snapshot"), AssetKey("exec_channel_cac_ltv"), AssetKey("exec_kpi_anomalies")}

    class Cursor:
        tables = set()

        def execute(self, sql):
            self.tables.add(sql.split(" from ")[1].split()[0].split(".")[-1].lower())

        def fetchall(self):
            return []

    digest.read_gold(Cursor())
    assert {AssetKey(table) for table in Cursor.tables} == waits_for      # what read_gold queries
