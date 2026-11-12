"""local/gold_diff.py: the comparison of two gold fingerprints that the CI scenario uses twice. A
pull request against main reports every difference and fails on none, Airflow against Dagster
fails on any difference."""
from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("gold_diff", REPO / "local" / "gold_diff.py")
gold_diff = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = gold_diff      # its dataclass looks its own module up by name
spec.loader.exec_module(gold_diff)

GOLD = {
    "exec_kpi_snapshot": {"rows": 3, "sums": {"revenue_ytd_usd": 210384.886315, "new_customers_mtd": 41}},
    "exec_web_channel_funnel": {"rows": 18, "sums": {"cost_per_purchase_usd": 512.37, "sessions": 90210}},
}


def changed(**tables) -> dict:
    gold = copy.deepcopy(GOLD)
    for table, values in tables.items():
        gold[table] = values
    return gold


def run(tmp_path, capsys, before, after, *flags) -> tuple[int, str]:
    """main() on two fingerprint files: its exit code and what it printed."""
    for name, gold in (("before.json", before), ("after.json", after)):
        (tmp_path / name).write_text(json.dumps(gold))
    code = gold_diff.main([*flags, str(tmp_path / "before.json"), str(tmp_path / "after.json")])
    return code, capsys.readouterr().out.rstrip("\n")


def test_identical_fingerprints_have_no_difference(tmp_path, capsys):
    code, text = run(tmp_path, capsys, GOLD, GOLD)
    assert code == 0
    assert text == "before and after have the same gold: 2 exec_* tables, every row count and every numeric sum equal."


def test_a_changed_sum_and_row_count_are_listed_with_their_change():
    after = changed(exec_kpi_snapshot={"rows": 4, "sums": {"revenue_ytd_usd": 210390.1, "new_customers_mtd": 41}})
    differences = gold_diff.compare(GOLD, after)
    assert [(d.table, d.what, d.before, d.after, d.change) for d in differences] == [
        ("exec_kpi_snapshot", "rows", 3, 4, "+1"),
        ("exec_kpi_snapshot", "revenue_ytd_usd", 210384.886315, 210390.1, "+5.213685"),
    ]


def test_a_new_table_a_removed_table_and_a_new_column_are_differences():
    after = changed(exec_mrr_monthly={"rows": 66, "sums": {"closing_mrr_usd": 1.5}})
    del after["exec_web_channel_funnel"]
    after["exec_kpi_snapshot"]["sums"]["active_subscriptions"] = 1200
    found = {(d.table, d.what): (d.before, d.after, d.change) for d in gold_diff.compare(GOLD, after)}
    assert found == {
        ("exec_mrr_monthly", "table (rows)"): ("absent", 66, "new"),
        ("exec_web_channel_funnel", "table (rows)"): (18, "absent", "removed"),
        ("exec_kpi_snapshot", "active_subscriptions"): ("absent", 1200, "new"),
    }


def test_a_null_sum_differs_from_a_number_and_equals_a_null():
    empty = changed(exec_kpi_snapshot={"rows": 0, "sums": {"revenue_ytd_usd": None, "new_customers_mtd": None}})
    assert gold_diff.compare(empty, empty) == []
    assert {d.what for d in gold_diff.compare(GOLD, empty)} == {"rows", "revenue_ytd_usd", "new_customers_mtd"}


def test_a_data_difference_exits_1_and_says_it_is_not_an_error(tmp_path, capsys):
    after = changed(exec_kpi_snapshot={"rows": 3, "sums": {"revenue_ytd_usd": 1.0, "new_customers_mtd": 41}})
    code, text = run(tmp_path, capsys, GOLD, after, "--markdown", "--names", "main", "this change")
    assert code == 1
    assert "Gold differs from main in 1 of 2 `exec_*` tables (1 differences)" in text
    assert "A difference is not an error" in text
    assert "| `exec_kpi_snapshot` | revenue_ytd_usd | 210,384.886315 | 1 | -210,383.886315 |" in text


def test_parity_tolerates_the_half_cent_tie_and_says_so(tmp_path, capsys):
    after = changed(exec_web_channel_funnel={"rows": 18, "sums": {"cost_per_purchase_usd": 512.38, "sessions": 90210}})
    code, text = run(tmp_path, capsys, GOLD, after, "--parity", "--names", "Airflow", "Dagster")
    assert code == 0
    assert "except for a rounding tie that known-pitfalls.md explains" in text
    assert "exec_web_channel_funnel.cost_per_purchase_usd: 512.37 -> 512.38  +0.01  (tolerated)" in text


def test_the_same_cent_is_a_difference_when_it_is_not_a_parity_check(tmp_path, capsys):
    after = changed(exec_web_channel_funnel={"rows": 18, "sums": {"cost_per_purchase_usd": 512.38, "sessions": 90210}})
    assert run(tmp_path, capsys, GOLD, after)[0] == 1


ANOMALIES = {"rows": 1890, "sums": {"baseline_median": 106664.926592, "spread": 44190.230454}}


def test_parity_tolerates_the_last_digit_of_the_sum_of_medians_and_says_so(tmp_path, capsys):
    before = changed(exec_kpi_anomalies=ANOMALIES)
    after = changed(exec_kpi_anomalies={"rows": 1890, "sums": {"baseline_median": 106664.926593, "spread": 44190.230454}})
    code, text = run(tmp_path, capsys, before, after, "--parity", "--names", "Airflow", "Dagster")
    assert code == 0
    assert "except for a rounding tie that known-pitfalls.md explains" in text
    assert "exec_kpi_anomalies.baseline_median: 106,664.926592 -> 106,664.926593  +0.000001  (tolerated)" in text
    assert run(tmp_path, capsys, before, after)[0] == 1         # against main it is listed as any difference is


@pytest.mark.parametrize("sums", [
    {"baseline_median": 106664.926594, "spread": 44190.230454},     # two digits: more than one tie explains
    {"baseline_median": 106664.926592, "spread": 44190.230455},     # the same digit of a column with no halves
])
def test_parity_fails_on_more_than_that_digit_and_on_another_column(tmp_path, capsys, sums):
    code, text = run(tmp_path, capsys, changed(exec_kpi_anomalies=ANOMALIES),
                     changed(exec_kpi_anomalies={"rows": 1890, "sums": sums}), "--parity", "--names", "Airflow", "Dagster")
    assert code == 1
    assert "Airflow and Dagster built different gold from the same data (1 differences):" in text


@pytest.mark.parametrize("sums", [
    {"cost_per_purchase_usd": 512.39, "sessions": 90210},      # two cents: more than one tie explains
    {"cost_per_purchase_usd": 512.37, "sessions": 90211},      # another column of the same table
])
def test_parity_fails_on_anything_else(tmp_path, capsys, sums):
    code, text = run(tmp_path, capsys, GOLD, changed(exec_web_channel_funnel={"rows": 18, "sums": sums}), "--parity",
                     "--names", "Airflow", "Dagster")
    assert code == 1
    assert "Airflow and Dagster built different gold from the same data (1 differences):" in text


def test_parity_fails_on_a_real_difference_beside_the_tolerated_cent(tmp_path, capsys):
    after = changed(
        exec_web_channel_funnel={"rows": 18, "sums": {"cost_per_purchase_usd": 512.38, "sessions": 90210}},
        exec_kpi_snapshot={"rows": 3, "sums": {"revenue_ytd_usd": 210384.886315, "new_customers_mtd": 42}},
    )
    code, text = run(tmp_path, capsys, GOLD, after, "--parity")
    assert code == 1
    assert "(1 differences)" in text and "(tolerated)" in text


def test_an_unreadable_fingerprint_exits_2(tmp_path, capsys):
    (tmp_path / "before.json").write_text("{not json")
    assert gold_diff.main([str(tmp_path / "before.json"), str(tmp_path / "missing.json")]) == 2
    assert "Cannot read a fingerprint" in capsys.readouterr().err
