"""The two engines are interchangeable: the same files give the same tables, whichever engine
processes each batch.

The three sample days are processed three times, in three lakehouses: by pandas alone, by Spark
alone, and alternating (pandas, Spark, pandas), so each engine also continues tables the other one
created and wrote. Bronze, silver and gold must be identical, except the columns that say who built
a row (contract.LINEAGE_COLUMNS), and each batch must count the same rows and drop the same ones.
The gate can then pick either engine for any batch, and nothing downstream can tell.
"""
from __future__ import annotations

import json

import pytest

pytest.importorskip("deltalake")
pytest.importorskip("pyspark")

import sample_days  # noqa: E402
from crm_lakehouse import delta_io, pandas_engine, planning, spark_engine  # noqa: E402
from crm_lakehouse.contract import BRONZE_EVENTS, GOLD_FUNNEL, LINEAGE_COLUMNS, SILVER_EVENTS  # noqa: E402
from crm_lakehouse.layout import Lakehouse  # noqa: E402

SCENARIOS = {
    "pandas": ("pandas", "pandas", "pandas"),
    "spark": ("spark", "spark", "spark"),
    "alternating": ("pandas", "spark", "pandas"),
}
BATCH_FIGURES = ("bronze_rows", "silver_inserted", "dropped_json", "gold_from", "gold_to", "gold_rows")


def process(lakehouse, spark, engines) -> list[dict]:
    results = []
    for day, engine in zip(sample_days.DAYS, engines):
        sample_days.land(lakehouse.landing, day)
        plan = planning.plan_batch(lakehouse, 1 << 30, forced=engine)
        assert plan.action == "run" and not plan.resumed
        if engine == "pandas":
            results.append(pandas_engine.run_batch(lakehouse, plan.batch_id))
        else:
            results.append(spark_engine.run_batch(spark, lakehouse, plan.batch_id))
        planning.record(lakehouse, plan.batch_id, "PUBLISHED", engine=engine)
    assert len({result["batch_id"] for result in results}) == len(engines)
    return results


def table(lakehouse, contract_table, key) -> list[dict]:
    rows = delta_io.read(lakehouse, contract_table).to_pylist()
    for row in rows:
        for column in LINEAGE_COLUMNS:
            row.pop(column, None)
    return sorted(rows, key=key)


def source_relative(rows, lakehouse):
    """Bronze names files by their full path, which differs between the three lakehouses."""
    for row in rows:
        row["_source_file"] = row["_source_file"].removeprefix(lakehouse.landing)
    return rows


@pytest.fixture(scope="module")
def runs(tmp_path_factory, spark):
    built = {}
    for name, engines in SCENARIOS.items():
        lakehouse = Lakehouse("parity", root=str(tmp_path_factory.mktemp(name)))
        built[name] = (lakehouse, process(lakehouse, spark, engines))
    return built


@pytest.mark.parametrize("scenario", ["spark", "alternating"])
def test_every_batch_counts_the_same_rows(runs, scenario):
    expected = [{k: r[k] for k in BATCH_FIGURES} for r in runs["pandas"][1]]
    actual = [{k: r[k] for k in BATCH_FIGURES} for r in runs[scenario][1]]
    assert actual == expected
    assert [r["engine"] for r in runs[scenario][1]] == list(SCENARIOS[scenario])


@pytest.mark.parametrize("scenario", ["spark", "alternating"])
def test_silver_and_gold_are_identical(runs, scenario):
    reference, _ = runs["pandas"]
    other, _ = runs[scenario]
    assert table(other, SILVER_EVENTS, lambda r: r["event_id"]) == table(reference, SILVER_EVENTS, lambda r: r["event_id"])
    gold_key = lambda r: (r["brand"], r["business_date"], r["channel"], r["device"])  # noqa: E731
    assert table(other, GOLD_FUNNEL, gold_key) == table(reference, GOLD_FUNNEL, gold_key)


@pytest.mark.parametrize("scenario", ["spark", "alternating"])
def test_bronze_keeps_every_line_alike(runs, scenario):
    def bronze(name):
        lakehouse, _ = runs[name]
        rows = source_relative(table(lakehouse, BRONZE_EVENTS, lambda r: json.dumps(r, sort_keys=True, default=str)), lakehouse)
        return sorted(rows, key=lambda r: json.dumps(r, sort_keys=True, default=str))

    assert bronze(scenario) == bronze("pandas")


def test_gold_says_which_engine_built_each_day(runs):
    lakehouse, results = runs["alternating"]
    engines = {(row["business_date"].isoformat(), row["_engine"]) for row in delta_io.read(lakehouse, GOLD_FUNNEL).to_pylist()}
    # 7 and 8 March were last rebuilt by the second batch (Spark), 9 March by the third (pandas)
    assert engines == {("2026-03-07", "spark"), ("2026-03-08", "spark"), ("2026-03-09", "pandas")}


def test_the_spark_job_refuses_a_batch_planned_by_another_release(lakehouse, spark, monkeypatch):
    sample_days.land(lakehouse.landing, "2026-03-07")
    plan = planning.plan_batch(lakehouse, 1 << 30, forced="spark")
    monkeypatch.setattr(spark_engine, "contract_fingerprint", lambda: "another-release")
    with pytest.raises(spark_engine.ContractMismatch):
        spark_engine.run_batch(spark, lakehouse, plan.batch_id)
    latest = planning.latest_states(planning.ledger(lakehouse))[plan.batch_id]
    assert latest["state"] == "FAILED" and "deploy the same release" in latest["error"]
    assert delta_io.read(lakehouse, BRONZE_EVENTS).num_rows == 0
