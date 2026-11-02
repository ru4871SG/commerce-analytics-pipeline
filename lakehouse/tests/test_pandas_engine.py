"""The in-process engine on the three sample days: what bronze, silver and gold hold after each
batch, worked out by hand from sample_days.py. tests/test_engine_parity.py then holds the Spark
engine to the same tables."""
from __future__ import annotations

import json
from datetime import date

import pytest

pytest.importorskip("deltalake")

import sample_days  # noqa: E402
from crm_lakehouse import delta_io, pandas_engine, planning  # noqa: E402
from crm_lakehouse.contract import BRONZE_EVENTS, GOLD_FUNNEL, SILVER_EVENTS  # noqa: E402

MARCH_7, MARCH_8, MARCH_9 = date(2026, 3, 7), date(2026, 3, 8), date(2026, 3, 9)
BUDGET = 1 << 30


def run_day(lakehouse, day, forced="pandas"):
    """Land the day, plan it, process it in process, and record it as published, as the
    orchestrator's publish step does once gold is in the warehouse."""
    sample_days.land(lakehouse.landing, day)
    plan = planning.plan_batch(lakehouse, BUDGET, forced=forced)
    assert plan.action == "run" and plan.engine == forced and not plan.resumed
    result = pandas_engine.run_batch(lakehouse, plan.batch_id)
    planning.record(lakehouse, plan.batch_id, "PUBLISHED", engine=forced)
    return result


def gold(lakehouse) -> dict:
    rows = delta_io.read(lakehouse, GOLD_FUNNEL).to_pylist()
    return {(r["brand"], r["business_date"], r["channel"], r["device"]):
            {k: v for k, v in r.items() if k not in ("brand", "business_date", "channel", "device", "_batch_id", "_engine")}
            for r in rows}


def silver(lakehouse) -> dict:
    return {r["event_id"]: r for r in delta_io.read(lakehouse, SILVER_EVENTS).to_pylist()}


FUNNEL_ZERO = dict(view_item_sessions=0, add_to_cart_sessions=0, checkout_sessions=0, purchase_sessions=0, purchases=0)


def test_the_first_day(lakehouse):
    result = run_day(lakehouse, "2026-03-07")
    assert result["bronze_rows"] == 8                       # e01 is in the file twice
    assert result["silver_inserted"] == 7
    assert json.loads(result["dropped_json"]) == {"parsable": 0, "has_ids": 0, "known_event_name": 0,
                                                  "valid_event_time": 0, "duplicates": 1}
    assert (result["gold_from"], result["gold_to"]) == (MARCH_7, MARCH_7)
    assert gold(lakehouse) == {
        ("broomx", MARCH_7, "facebook", "mobile"): dict(
            sessions=1, engaged_sessions=1, view_item_sessions=1, add_to_cart_sessions=1, checkout_sessions=1,
            purchase_sessions=1, purchases=1, events=6),
        # s2 has one page view so far: not engaged yet
        ("broomx", MARCH_7, "direct", "desktop"): dict(sessions=1, engaged_sessions=0, **FUNNEL_ZERO, events=1),
    }
    assert result["observed_pandas_bytes"] > 0


def test_a_late_day_rebuilds_the_day_its_sessions_started(lakehouse):
    run_day(lakehouse, "2026-03-07")
    result = run_day(lakehouse, "2026-03-08")
    assert result["bronze_rows"] == 10
    assert result["silver_inserted"] == 9                   # e07 was sent again: silver has it already
    # s2 and s5 started on 7 March: that day is rebuilt with them
    assert (result["gold_from"], result["gold_to"]) == (MARCH_7, MARCH_8)
    funnel = gold(lakehouse)
    assert funnel[("broomx", MARCH_7, "direct", "desktop")] == dict(sessions=1, engaged_sessions=1, **FUNNEL_ZERO, events=2)
    assert funnel[("greenz", MARCH_7, "instagram", "mobile")] == dict(sessions=1, engaged_sessions=1, **FUNNEL_ZERO, events=3)
    # Across the change to summer time, still 8 March; the channel is trimmed and lower-cased
    assert funnel[("wecare", MARCH_8, "google", "tablet")] == dict(
        sessions=1, engaged_sessions=1, **{**FUNNEL_ZERO, "view_item_sessions": 1}, events=3)
    assert funnel[("wecare", MARCH_8, "newsletter", "unknown")] == dict(sessions=1, engaged_sessions=0, **FUNNEL_ZERO, events=2)
    assert len(funnel) == 5
    # The first day's other rows are unchanged
    assert funnel[("broomx", MARCH_7, "facebook", "mobile")]["events"] == 6


def test_the_rules_drop_what_they_should_and_odd_values_are_read_alike(lakehouse):
    for day in sample_days.DAYS:
        result = run_day(lakehouse, day)
    assert result["bronze_rows"] == 17
    assert json.loads(result["dropped_json"]) == {"parsable": 1, "has_ids": 3, "known_event_name": 1,
                                                  "valid_event_time": 3, "duplicates": 2}
    assert result["silver_inserted"] == 7
    events = silver(lakehouse)
    assert len(events) == 23
    assert events["e06"]["customer_id"] == 42 and events["e06"]["order_number"] == "SM-1"
    assert events["e10"]["variant_id"] == 17                       # sent as a string
    assert events["e22"]["variant_id"] == 9                        # params sent as a JSON string
    assert events["e22"]["page_path"] == "/productos/café"
    assert events["e12"]["customer_id"] is None                    # 1e3
    assert events["e17"]["customer_id"] is None                    # +5
    assert events["e22"]["customer_id"] is None                    # Arabic-Indic digits
    assert events["e23"]["customer_id"] == 7
    assert events["e21"]["engagement_time_msec"] == 4000           # text after the object is ignored
    assert {events[e]["device"] for e in ("e17", "e19", "e20")} == {"mobile"}
    assert events["e12"]["device"] == "unknown" and events["e09"]["channel"] == "google"
    assert events["e07"]["business_date"] == MARCH_7 and events["e08"]["business_date"] == MARCH_8
    assert events["e11"]["business_date"] == MARCH_8               # 03:10 summer time
    assert gold(lakehouse)[("greenz", MARCH_9, "direct", "mobile")] == dict(
        sessions=1, engaged_sessions=1, view_item_sessions=1, add_to_cart_sessions=1, checkout_sessions=0,
        purchase_sessions=0, purchases=0, events=7)
    bronze = delta_io.read(lakehouse, BRONZE_EVENTS).to_pylist()
    assert sum(1 for row in bronze if row["_corrupt_record"]) == 1


def test_a_batch_retried_after_bronze_was_written_does_not_load_it_twice(lakehouse, monkeypatch):
    sample_days.land(lakehouse.landing, "2026-03-07")
    plan = planning.plan_batch(lakehouse, BUDGET, forced="pandas")

    def fail(*args, **kwargs):
        raise RuntimeError("the worker died during the merge")

    monkeypatch.setattr(delta_io, "merge_insert_new", fail)
    with pytest.raises(RuntimeError):
        pandas_engine.run_batch(lakehouse, plan.batch_id)
    monkeypatch.undo()
    assert len(delta_io.read(lakehouse, BRONZE_EVENTS)) == 8      # bronze committed before the failure

    again = planning.plan_batch(lakehouse, BUDGET, forced="pandas")
    assert again.batch_id == plan.batch_id and again.resumed
    result = pandas_engine.run_batch(lakehouse, again.batch_id)
    assert result["bronze_rows"] == 8 and result["silver_inserted"] == 7
    assert len(delta_io.read(lakehouse, BRONZE_EVENTS)) == 8      # not appended twice
    assert planning.plan_batch(lakehouse, BUDGET).action == "publish"


def test_running_a_finished_day_again_changes_nothing(lakehouse):
    for day in sample_days.DAYS:
        run_day(lakehouse, day)
    before = gold(lakehouse)
    # The same files, landed again under the same names, are already in bronze
    for day in sample_days.DAYS:
        sample_days.land(lakehouse.landing, day)
    assert planning.pending_files(lakehouse) == []
    assert gold(lakehouse) == before


def test_a_file_without_a_line_is_loaded_once(lakehouse):
    import gzip
    import os

    empty = os.path.join(lakehouse.landing, "2026-03-06", "quiet-2026-03-06.json.gz")
    os.makedirs(os.path.dirname(empty))
    with gzip.open(empty, "wt") as out:
        out.write("\n")                                       # a day without traffic
    result = run_day(lakehouse, "2026-03-07")
    assert result["files"] == 2 and result["bronze_rows"] == 8
    # bronze holds no row of it, but its batch succeeded: it is not planned again
    assert planning.pending_files(lakehouse) == []
