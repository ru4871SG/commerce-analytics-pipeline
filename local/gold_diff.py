#!/usr/bin/env python3
"""Compare two gold fingerprints, which exec_* tables changed, and how.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

# (table, column) -> the largest difference between two builds of the same data
ROUNDING_TIES = {
    ("exec_web_channel_funnel", "cost_per_purchase_usd"): 0.01,     # a cent: the column is rounded to cents
    ("exec_kpi_anomalies", "baseline_median"): 0.000001,            # the last digit the fingerprint keeps
}
ABSENT = "absent"       # the table, or the column's sum, is not in that fingerprint
NUMBER = (int, float)


@dataclass(frozen=True)
class Difference:
    table: str
    what: str               # "table (rows)" for a table on one side only, "rows", or a column name
    before: object          # a row count or a sum (None: the sum is null), or ABSENT
    after: object
    tolerated: bool = False

    @property
    def change(self) -> str:
        if self.before == ABSENT:
            return "new"
        if self.after == ABSENT:
            return "removed"
        if isinstance(self.before, NUMBER) and isinstance(self.after, NUMBER):
            delta = self.after - self.before
            return f"{delta:+,}" if isinstance(delta, int) else f"{delta:+,.6f}".rstrip("0").rstrip(".")
        return ""


def _shown(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool) or not isinstance(value, NUMBER):
        return str(value)
    return f"{value:,}" if isinstance(value, int) else f"{value:,.6f}".rstrip("0").rstrip(".")


def compare(before: dict, after: dict, parity: bool = False) -> list[Difference]:
    """Every difference between two fingerprints: tables in name order, rows first, then columns."""
    differences = []
    for table in sorted(set(before) | set(after)):
        old, new = before.get(table), after.get(table)
        if old is None or new is None:
            # A whole table: its row count stands for it
            differences.append(Difference(table, "table (rows)", old["rows"] if old else ABSENT,
                                          new["rows"] if new else ABSENT))
            continue
        if old["rows"] != new["rows"]:
            differences.append(Difference(table, "rows", old["rows"], new["rows"]))
        for column in sorted(set(old["sums"]) | set(new["sums"])):
            # ABSENT on one side: a column added or removed, or cast to a type the fingerprint sums
            old_sum, new_sum = old["sums"].get(column, ABSENT), new["sums"].get(column, ABSENT)
            if old_sum == new_sum:
                continue
            limit = ROUNDING_TIES.get((table, column)) if parity else None
            tolerated = (limit is not None and isinstance(old_sum, NUMBER) and isinstance(new_sum, NUMBER)
                         and abs(new_sum - old_sum) <= limit + 1e-9)
            differences.append(Difference(table, column, old_sum, new_sum, tolerated))
    return differences


def render(differences: list[Difference], names: tuple[str, str], tables: int, parity: bool, markdown: bool) -> str:
    """One sentence that says what the comparison found, then the differences as a table."""
    before, after = names
    failing = [d for d in differences if not d.tolerated]
    if not differences:
        lines = [f"{before} and {after} have the same gold: {tables} `exec_*` tables, every row count and every "
                 "numeric sum equal."]
    elif parity and not failing:
        lines = [f"{before} and {after} have the same gold, except for a rounding tie that "
                 "known-pitfalls.md explains (a float sum exactly between two rounded values):"]
    elif parity:
        lines = [f"**{before} and {after} built different gold from the same data** ({len(failing)} differences):"]
    else:
        changed = len({d.table for d in differences})
        lines = [f"Gold differs from {before} in {changed} of {tables} `exec_*` tables ({len(differences)} "
                 "differences). A difference is not an error: check that each one is what the change intends."]
    if differences and markdown:
        lines += ["", f"| Table | What | {before} | {after} | Change |", "|---|---|---:|---:|---:|"]
        lines += [f"| `{d.table}` | {d.what}{' (tolerated)' if d.tolerated else ''} | {_shown(d.before)} "
                  f"| {_shown(d.after)} | {d.change} |" for d in differences]
    elif differences:
        lines += [f"  {d.table}.{d.what}: {_shown(d.before)} -> {_shown(d.after)}  {d.change}"
                  f"{'  (tolerated)' if d.tolerated else ''}" for d in differences]
    text = "\n".join(lines)
    return text if markdown else text.replace("`", "").replace("**", "")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare two gold fingerprints.")
    parser.add_argument("before", type=Path)
    parser.add_argument("after", type=Path)
    parser.add_argument("--parity", action="store_true",
                        help="both are builds of the same data: a difference is an error, the known rounding ties aside")
    parser.add_argument("--markdown", action="store_true", help="a Markdown table, for a job summary")
    parser.add_argument("--names", nargs=2, metavar=("BEFORE", "AFTER"), default=("before", "after"))
    args = parser.parse_args(argv)
    try:
        before, after = (json.loads(path.read_text()) for path in (args.before, args.after))
    except (OSError, ValueError) as error:
        print(f"Cannot read a fingerprint: {error}", file=sys.stderr)
        return 2
    differences = compare(before, after, args.parity)
    print(render(differences, tuple(args.names), len(set(before) | set(after)), args.parity, args.markdown))
    return 1 if any(not d.tolerated for d in differences) else 0


if __name__ == "__main__":
    raise SystemExit(main())
