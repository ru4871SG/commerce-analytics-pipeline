"""Checks the DAG must pass before it is deployed. They parse the DAG file; no database or
connection is needed."""
import shlex
import sys
from types import SimpleNamespace

import pytest

pytest.importorskip("airflow")

from crm_pipeline import cdc, dbt, fx_rates
from crm_pipeline.assets import ACTIVITY, GOLD
from crm_pipeline.config import INCREMENTAL_TABLES, brands_to_extract
from crm_pipeline.errors import PermanentError


def test_every_dag_imports(dagbag):
    assert dagbag.import_errors == {}


def test_dags_are_tagged_and_only_the_simulator_backfills(dagbag):
    assert set(dagbag.dags) == {"crm_executive_report", "crm_daily_activity", "crm_executive_activation"}
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
    # Missed report runs are not worth replaying; missed days of activity are
    assert not dagbag.dags["crm_executive_report"].catchup
    assert not dagbag.dags["crm_executive_activation"].catchup
    assert dagbag.dags["crm_daily_activity"].catchup


def test_report_runs_when_the_days_activity_is_in(dag):
    assert [asset.name for asset in dag.timetable.asset_condition.objects] == [ACTIVITY.name]


def test_simulator_writes_each_brand_after_the_previous_day(dagbag):
    simulator = dagbag.dags["crm_daily_activity"]
    simulate = simulator.get_task("simulate_brand_day")
    assert simulate.depends_on_past                       # a day starts from the day before
    assert str(simulator.timezone) == "America/New_York"
    assert [a.name for a in simulator.get_task("activity_recorded").outlets] == [ACTIVITY.name]
    assert simulator.get_task("activity_recorded").upstream_task_ids == {"simulate_brand_day"}


def test_one_ingest_group_per_extracted_brand(dag):
    for brand in brands_to_extract():
        group = [task.task_id for task in dag.tasks if task.task_id.startswith(f"ingest_{brand.key}.")]
        assert sorted(group) == [f"ingest_{brand.key}.load_tables", f"ingest_{brand.key}.prepare"]


def test_each_brand_is_prepared_then_loaded_in_one_task(dag):
    # One task reads the brand's nine tables in one snapshot: an order line and the product
    # variant it points at always come from the same moment of the source
    for brand in brands_to_extract():
        group = f"ingest_{brand.key}"
        assert dag.get_task(f"{group}.load_tables").upstream_task_ids == {f"{group}.prepare"}
    loads = [task for task in dag.tasks if task.task_id.endswith(".load_tables")]
    assert len(loads) <= dag.max_active_tasks        # every brand extracts at once


def test_merges_follow_the_foreign_keys():
    names = {table.name for table in INCREMENTAL_TABLES}
    for position, table in enumerate(INCREMENTAL_TABLES):
        # a parent is an incremental table listed earlier, so the graph has no cycle
        assert set(table.merge_after) <= {t.name for t in INCREMENTAL_TABLES[:position]} <= names
    parents = {table.name: table.merge_after for table in INCREMENTAL_TABLES}
    assert parents["orders"] == ("customers",)
    assert parents["order_items"] == parents["refund_payments"] == ("orders",)


def test_merges_wait_for_the_loads_then_reconcile_then_dbt(dag):
    loads = {f"ingest_{b.key}.load_tables" for b in brands_to_extract()}
    leaves = set()
    for table in INCREMENTAL_TABLES:
        merge = f"streams_and_tasks.merge_{table.name}"
        parents = {f"streams_and_tasks.merge_{p}" for p in table.merge_after}
        # The first merges wait for every load; the others for the merges before them
        assert dag.get_task(merge).upstream_task_ids == (parents or loads)
        # A merge task with nothing to merge is skipped; the ones after it still run
        assert dag.get_task(merge).trigger_rule == "none_failed"
        if not any(table.name in other.merge_after for other in INCREMENTAL_TABLES):
            leaves.add(merge)
    assert leaves == {"streams_and_tasks.merge_order_items", "streams_and_tasks.merge_refund_payments",
                      "streams_and_tasks.merge_marketing_spend"}
    reconcile = dag.get_task("reconcile_bronze")
    assert reconcile.upstream_task_ids == leaves
    assert reconcile.trigger_rule == "none_failed"
    # dbt does not wait for the clickstream: a failed batch must not stop the report
    assert dag.get_task("dbt_build").upstream_task_ids == {"reconcile_bronze"}


def test_the_gate_branches_to_one_engine_then_publishes(dag):
    group = {task.task_id for task in dag.tasks if task.task_id.startswith("lakehouse.")}
    assert group == {f"lakehouse.{name}" for name in
                     ("plan_web_batch", "choose_web_engine", "run_in_process", "run_on_spark", "publish_web_funnel")}
    branch = dag.get_task("lakehouse.choose_web_engine")
    assert branch.upstream_task_ids == {"lakehouse.plan_web_batch"}
    assert branch.downstream_task_ids == {"lakehouse.run_in_process", "lakehouse.run_on_spark",
                                          "lakehouse.publish_web_funnel"}
    publish = dag.get_task("lakehouse.publish_web_funnel")
    # The engine the gate did not choose is skipped; publishing runs after the one it did
    assert publish.trigger_rule == "none_failed_min_one_success"
    assert publish.upstream_task_ids == {"lakehouse.plan_web_batch", "lakehouse.choose_web_engine",
                                         "lakehouse.run_in_process", "lakehouse.run_on_spark"}
    # The lakehouse does not wait for the CRM, nor the CRM for it
    assert dag.get_task("lakehouse.plan_web_batch").upstream_task_ids == set()
    assert publish.downstream_task_ids == set()


def test_the_fx_load_is_a_side_branch_that_nothing_waits_for(dag):
    group = {task.task_id for task in dag.tasks if task.task_id.startswith("side_loads.")}
    assert group == {"side_loads.load_fx_rates"} | {
        f"side_loads.capture_orders_cdc_{brand.key}" for brand in brands_to_extract()}
    load = dag.get_task("side_loads.load_fx_rates")
    # It waits for nothing, and nothing waits for it: when it fails, the run fails and the report
    # is still built, with the last exchange rate carried forward
    assert load.upstream_task_ids == set()
    assert load.downstream_task_ids == set()
    assert dag.get_task("dbt_build").upstream_task_ids == {"reconcile_bronze"}


def test_without_its_connection_the_fx_load_is_skipped_not_failed(dag, monkeypatch):
    from airflow.sdk.exceptions import AirflowSkipException

    load = dag.get_task("side_loads.load_fx_rates").python_callable
    monkeypatch.setattr(fx_rates, "load", lambda: {"state": "SKIPPED", "reason": "No ecb_fx_api connection", "added": 0})
    with pytest.raises(AirflowSkipException, match="No ecb_fx_api connection"):
        load()
    # With the connection it returns what it did, whether a rate was new or not
    for result in ({"state": "LOADED", "added": 2}, {"state": "UP_TO_DATE", "added": 0}):
        monkeypatch.setattr(fx_rates, "load", lambda result=result: result)
        assert load() == result


def test_each_brand_s_orders_are_captured_from_the_log_after_its_load_and_nothing_waits_for_it(dag):
    loads = {f"ingest_{brand.key}.load_tables" for brand in brands_to_extract()}
    for brand in brands_to_extract():
        capture = dag.get_task(f"side_loads.capture_orders_cdc_{brand.key}")
        # After that brand's load: every version the loader copied is then in the log the capture reads
        assert capture.upstream_task_ids == {f"ingest_{brand.key}.load_tables"}
        # A side load: when it fails, the run fails and the report is still built
        assert capture.downstream_task_ids == set()
    # The merges still wait for the loads and nothing else, and dbt for the reconciliation
    first_merges = {f"streams_and_tasks.merge_{table.name}" for table in INCREMENTAL_TABLES if not table.merge_after}
    for merge in first_merges:
        assert dag.get_task(merge).upstream_task_ids == loads
    assert dag.get_task("reconcile_bronze").upstream_task_ids.isdisjoint(
        {task.task_id for task in dag.tasks if task.task_id.startswith("side_loads.")})
    assert dag.get_task("dbt_build").upstream_task_ids == {"reconcile_bronze"}


def test_without_its_connection_the_capture_is_skipped_and_a_lost_slot_fails_without_a_retry(dag, monkeypatch):
    from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

    capture = dag.get_task("side_loads.capture_orders_cdc_broomx").python_callable
    calls = []

    def skipped(brand, database):
        calls.append((brand, database))
        return {"state": "SKIPPED", "reason": "No crm_source_cdc connection", "changes": 0}

    monkeypatch.setattr(cdc, "capture", skipped)
    with pytest.raises(AirflowSkipException, match="No crm_source_cdc connection"):
        capture("broomx", "crm_broomx")
    assert calls == [("broomx", "crm_broomx")]
    # With the connection it returns what it did, whether the log held a change or not
    for result in ({"state": "CAPTURED", "changes": 12}, {"state": "UP_TO_DATE", "changes": 0}):
        monkeypatch.setattr(cdc, "capture", lambda brand, database, result=result: result)
        assert capture("broomx", "crm_broomx") == result

    def lost(brand, database):
        raise PermanentError("The replication slot cdc_crm_broomx was invalidated (wal_removed)")

    # A slot that is gone stays gone: Airflow does not retry an AirflowFailException
    monkeypatch.setattr(cdc, "capture", lost)
    with pytest.raises(AirflowFailException, match="was invalidated"):
        capture("broomx", "crm_broomx")


def test_dbt_creates_the_side_loads_tables_before_anything_else(dag, monkeypatch):
    # No side load is upstream of dbt_build: one that has never run must leave dbt an empty
    # source table, not a missing one
    calls = []
    monkeypatch.setattr(dbt.raw_tables, "ensure_side_tables", lambda: calls.append("side tables"))
    monkeypatch.setattr(dbt, "build", lambda *args, **kwargs: calls.append("freshness and build"))
    monkeypatch.setattr(dbt, "get_current_context", lambda: {"ti": SimpleNamespace(try_number=1)})
    monkeypatch.setattr(dbt, "airflow_environment", lambda: {})
    dag.get_task("dbt_build").python_callable()
    assert calls == ["side tables", "freshness and build"]


def test_the_gate_decides_unless_the_run_says_otherwise(dag):
    assert dag.params["web_engine"] == "auto"


def test_the_stream_is_read_by_its_merge_task(dag):
    # Reading the stream only logs it, so it is not a task of its own
    assert not [task.task_id for task in dag.tasks if task.task_id.endswith("_changes_stream")]
    assert len([task for task in dag.tasks if task.task_id.startswith("streams_and_tasks.")]) == len(INCREMENTAL_TABLES)


def test_resyncing_bronze_is_opt_in(dag):
    assert dag.params["resync_bronze"] is False


def test_gold_announces_its_asset(dag):
    assert [a.name for a in dag.get_task("dbt_build").outlets] == ["crm_executive_report_gold"]


def test_activation_runs_when_gold_is_rebuilt(dagbag):
    activation = dagbag.dags["crm_executive_activation"]
    assert [asset.name for asset in activation.timetable.asset_condition.objects] == [GOLD.name]
    assert activation.get_task("publish_executive_digest").upstream_task_ids == {"build_executive_digest"}
    superset = activation.get_task("verify_superset_dashboard")
    assert superset.upstream_task_ids == set()
    assert superset.skip_on_exit_code == [99]        # Superset not running: skipped, not failed
    # Airflow's interpreter, which has the script's packages, not whichever python3 is on the PATH
    assert shlex.split(superset.bash_command)[0] == sys.executable
    for task in activation.tasks:
        assert task.on_failure_callback, task.task_id


def test_retries_and_alerts(dag):
    assert dag.max_active_runs == 1
    for task in dag.tasks:
        assert task.retries >= 2, task.task_id
        assert task.on_failure_callback, task.task_id
    # An export already running answers 429: give it time to finish
    assert dag.get_task("ingest_broomx.prepare").retries == 4


TARGET = "target/airflow/dbt_build"


def dbt_commands(monkeypatch, tmp_path, try_number, target="prod", results_left=False):
    """The dbt commands dbt.build runs, in a project folder of its own. results_left: the try
    before left run results behind."""
    (tmp_path / "dbt_packages" / "dbt_utils").mkdir(parents=True)
    results = tmp_path / TARGET / "run_results.json"
    results.parent.mkdir(parents=True)
    if results_left:
        results.write_text("{}")
    monkeypatch.setattr(dbt, "DBT_PROJECT_DIR", tmp_path)
    commands = []
    dbt.build(try_number, TARGET, True, {"DBT_TARGET": target},
              run=lambda args, target_path, environment: commands.append((args, target_path, results.exists())))
    return commands


def test_one_dbt_task_checks_freshness_then_builds_silver_and_gold(dag, monkeypatch, tmp_path):
    assert dag.get_task("dbt_build").task_type == "_PythonDecoratedOperator"
    commands = dbt_commands(monkeypatch, tmp_path, 1)
    # A stale bronze stops the task before anything is built; then every layer, in one build
    assert [args for args, _, _ in commands] == [["source", "freshness"], ["build"]]
    assert [path for _, path, _ in commands] == [f"{TARGET}/source_freshness", TARGET]


def test_dbt_resumes_with_dbt_retry_on_an_airflow_retry(monkeypatch, tmp_path):
    # The first try removes the previous run's results, so a retry never resumes yesterday's run
    first = dbt_commands(monkeypatch, tmp_path / "first", 1, results_left=True)
    assert [args for args, _, _ in first] == [["source", "freshness"], ["build"]] and not first[0][2]
    retry = dbt_commands(monkeypatch, tmp_path / "retry", 2, results_left=True)
    assert [args for args, _, _ in retry] == [["source", "freshness"], ["retry"]]     # checked again on every try
    # When the failed try left no run results, the retry builds
    rebuilt = dbt_commands(monkeypatch, tmp_path / "rebuilt", 2)
    assert [args for args, _, _ in rebuilt] == [["source", "freshness"], ["build"]]


def test_emulator_builds_exclude_unit_tests(monkeypatch, tmp_path):
    assert dbt_commands(monkeypatch, tmp_path / "prod", 1, "prod")[-1][0] == ["build"]
    emulator = ["build", "--exclude-resource-type", "unit_test"]
    assert dbt_commands(monkeypatch, tmp_path / "emulator", 1, "emulator")[-1][0] == emulator
    assert dbt_commands(monkeypatch, tmp_path / "fallback", 2, "emulator")[-1][0] == emulator   # a retry's fallback


def test_dbt_retry_gets_no_build_flags(monkeypatch, tmp_path):
    # dbt retry repeats the failed build with its own flags and rejects --exclude-resource-type
    assert dbt_commands(monkeypatch, tmp_path, 2, "emulator", results_left=True)[-1][0] == ["retry"]


def test_dbt_gets_the_connection_and_the_airflow_variables(monkeypatch):
    monkeypatch.setattr(dbt, "dbt_environment", lambda: {"SNOWFLAKE_USER": "fake", "DBT_TARGET": "emulator"})
    monkeypatch.setattr(dbt.Variable, "get", lambda key, default=None: {"crm_dbt_target": "prod"}.get(key, default))
    environment = dbt.airflow_environment()
    assert environment == {"SNOWFLAKE_USER": "fake", "DBT_TARGET": "prod", "DBT_DEV_SCHEMA": "dbt_dev"}
