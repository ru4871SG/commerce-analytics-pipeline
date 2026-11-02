"""The gate and the planner: which engine a batch gets, and why; what the next plan is after a
batch stopped half-way."""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

pytest.importorskip("deltalake")

from crm_lakehouse import planning  # noqa: E402
from crm_lakehouse.planning import BatchProfile, Calibration, LandingFile  # noqa: E402

MIB = planning.MIB
BUDGET = 1024 * MIB          # the default: a small file stays in process


def batch(pandas_mib: float) -> BatchProfile:
    return BatchProfile(files=3, compressed_bytes=2 * MIB, estimated_rows=10_000,
                        estimated_pandas_bytes=int(pandas_mib * MIB))


def test_a_batch_within_the_budget_stays_in_process():
    decision = planning.choose_engine(batch(900), budget_bytes=1024 * MIB)
    assert decision.engine == "pandas"
    assert decision.reason.startswith("fits in process: 3 files, 2.0 MiB compressed, ~10,000 rows, ~900 MiB")
    assert "within the 1,024 MiB budget" in decision.reason


def test_a_batch_over_the_budget_goes_to_spark():
    decision = planning.choose_engine(batch(1025), budget_bytes=1024 * MIB)
    assert decision.engine == "spark"
    assert decision.reason.startswith("too big for pandas") and "over the 1,024 MiB budget" in decision.reason


def test_the_runs_parameter_overrides_the_estimate_and_says_so():
    assert planning.choose_engine(batch(10), 1024 * MIB, forced="spark").engine == "spark"
    decision = planning.choose_engine(batch(5000), 1024 * MIB, forced="pandas")
    assert decision.engine == "pandas" and decision.reason.startswith("forced by the run's engine parameter")
    with pytest.raises(ValueError):
        planning.choose_engine(batch(1), 1024 * MIB, forced="duckdb")


def test_the_estimate_scales_with_the_compressed_bytes_and_the_peak_factor():
    calibration = Calibration(rows_per_compressed_byte=1 / 500, pandas_bytes_per_row=3000, batches=0)
    profile = planning.profile([LandingFile("a", 500_000), LandingFile("b", 500_000)], calibration)
    assert profile == BatchProfile(2, 1_000_000, 2_000, planning.PEAK_OVERHEAD_BYTES + 2_000 * 3000 * planning.PEAK_FACTOR)


def ledger_row(**values):
    row = dict(state="SUCCEEDED", compressed_bytes=None, bronze_rows=None, observed_pandas_bytes=None)
    row.update(values)
    return row


def test_calibration_starts_from_the_defaults_then_learns_from_the_batches():
    defaults = planning.calibrate([])
    assert defaults.batches == 0
    assert defaults.rows_per_compressed_byte == planning.DEFAULT_ROWS_PER_COMPRESSED_BYTE
    rows = [
        ledger_row(compressed_bytes=1_000, bronze_rows=4, observed_pandas_bytes=8_000),    # 2,000 B a row
        ledger_row(compressed_bytes=3_000, bronze_rows=8, observed_pandas_bytes=24_000),   # 3,000 B a row
        ledger_row(compressed_bytes=6_000, bronze_rows=12),                                 # Spark: no pandas size
        ledger_row(state="FAILED", compressed_bytes=10**9, bronze_rows=1),                  # not counted
    ]
    learned = planning.calibrate(rows)
    assert learned.batches == 3
    assert learned.rows_per_compressed_byte == pytest.approx(24 / 10_000)
    assert learned.pandas_bytes_per_row == pytest.approx(2_500)        # the median of the pandas batches


def test_batch_ids_are_unique_per_moment_and_file_list():
    now = datetime(2026, 10, 28, 7, 30, tzinfo=timezone.utc)
    one = planning.batch_id_for([LandingFile("a", 1)], now)
    assert one.startswith("20261028T073000Z-")
    assert one != planning.batch_id_for([LandingFile("b", 1)], now)


def landing_files(lakehouse, names_and_sizes):
    import os

    for name, size in names_and_sizes:
        path = os.path.join(lakehouse.landing, name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as out:
            out.write(b"x" * size)


def test_the_planner_leaves_temporary_files_and_other_formats_for_later(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 10), ("2026-10-27/.b.json.gz", 10),
                              ("2026-10-27/c.csv", 10), ("_tmp/d.json.gz", 10)])
    assert [file.path.rsplit("/", 1)[-1] for file in planning.pending_files(lakehouse)] == ["a.json.gz"]


def test_nothing_to_do_when_no_file_is_waiting(lakehouse):
    plan = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    assert plan.action == "nothing"


def test_the_plan_is_recorded_before_anything_runs(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 2_000)])
    plan = planning.plan_batch(lakehouse, budget_bytes=BUDGET, orchestrator="airflow", run_id="r1")
    assert plan.action == "run" and plan.engine == "pandas" and not plan.resumed
    planned = planning.planned_row(lakehouse, plan.batch_id)
    assert planned["engine"] == "pandas" and planned["reason"] == plan.reason
    assert planning.batch_files(planned) == [f"{lakehouse.landing}/2026-10-27/a.json.gz"]
    assert planned["orchestrator"] == "airflow" and planned["contract"]


def test_a_batch_that_never_started_runs_on_its_engine_again(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 2_000)])
    first = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    again = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    assert again.batch_id == first.batch_id and again.engine == first.engine and again.resumed


def test_a_batch_that_died_in_process_moves_to_spark(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 2_000)])
    first = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    planning.record(lakehouse, first.batch_id, "RUNNING", engine="pandas")    # then the worker was killed
    again = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    assert again.batch_id == first.batch_id and again.engine == "spark"
    assert again.reason.startswith("escalated: the in-process attempt")
    assert planning.planned_row(lakehouse, first.batch_id)["engine"] == "spark"


def test_a_batch_that_failed_on_spark_runs_there_again(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 2_000)])
    first = planning.plan_batch(lakehouse, budget_bytes=BUDGET, forced="spark")
    planning.record(lakehouse, first.batch_id, "FAILED", engine="spark", error="cluster lost")
    again = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    assert again.batch_id == first.batch_id and again.engine == "spark"


def test_a_succeeded_batch_is_published_before_the_next_one_is_planned(lakehouse):
    landing_files(lakehouse, [("2026-10-27/a.json.gz", 2_000)])
    first = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    planning.record(lakehouse, first.batch_id, "SUCCEEDED", engine="pandas")
    landing_files(lakehouse, [("2026-10-28/b.json.gz", 2_000)])
    assert planning.plan_batch(lakehouse, budget_bytes=BUDGET).action == "publish"
    planning.record(lakehouse, first.batch_id, "PUBLISHED", engine="pandas")
    following = planning.plan_batch(lakehouse, budget_bytes=BUDGET)
    # a.json.gz left no row in bronze, but its batch succeeded: only b is new
    assert following.action == "run" and following.batch_id != first.batch_id and following.files == 1
