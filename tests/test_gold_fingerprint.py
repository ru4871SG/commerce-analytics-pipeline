"""local/gold_fingerprint.py: what the regression fingerprint of gold covers. Every before/after
comparison in the skill rests on it, so what it sums, counts and leaves out is fixed here."""
from __future__ import annotations

import importlib.util
from decimal import Decimal
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("gold_fingerprint", REPO / "local" / "gold_fingerprint.py")
gold_fingerprint = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gold_fingerprint)

COLUMNS = {
    "EXEC_KPI_ANOMALIES": [("BRAND", "TEXT"), ("DATE_DAY", "DATE"), ("VALUE", "FLOAT"), ("ROBUST_Z", "NUMBER"),
                           ("IS_ANOMALY", "BOOLEAN"), ("NEW_CUSTOMERS", "HUGEINT")],
    "EXEC_NAMES_ONLY": [("BRAND", "TEXT")],
}


class Warehouse:
    """A cursor that answers the fingerprint's three kinds of query and keeps the last one."""

    def __init__(self):
        self.statements = []

    def execute(self, sql, parameters=None):
        self.statements.append(sql)
        self.asked, self.parameters = sql, parameters

    def fetchall(self):
        if "information_schema.tables" in self.asked:
            return [(table,) for table in COLUMNS]
        return COLUMNS[self.parameters[0]]

    def fetchone(self):
        if "EXEC_NAMES_ONLY" in self.asked:
            return (3, None)
        return (1890, 123.456789123, Decimal("17.50"), Decimal("19"))


def test_numbers_are_summed_flags_are_counted_and_the_rest_is_left_out():
    cursor = Warehouse()
    result = gold_fingerprint.fingerprint(cursor)
    assert result["exec_kpi_anomalies"] == {
        "rows": 1890,
        "sums": {"value": 123.456789, "robust_z": 17.5, "is_anomaly": 19},
    }
    query = next(sql for sql in cursor.statements if 'from ANALYTICS.GOLD."EXEC_KPI_ANOMALIES"' in sql)
    assert query == ('select count(*), sum("VALUE") as "VALUE", sum("ROBUST_Z") as "ROBUST_Z", '
                     'sum(iff("IS_ANOMALY", 1, 0)) as "IS_ANOMALY" from ANALYTICS.GOLD."EXEC_KPI_ANOMALIES"')
    assert not {"brand", "date_day", "new_customers"} & set(result["exec_kpi_anomalies"]["sums"])


def test_a_table_with_nothing_to_sum_still_has_its_row_count():
    assert gold_fingerprint.fingerprint(Warehouse())["exec_names_only"] == {"rows": 3, "sums": {}}


def test_only_the_reporting_tables_are_fingerprinted():
    cursor = Warehouse()
    gold_fingerprint.gold_tables(cursor)
    assert "table_schema = 'GOLD' and table_name like 'EXEC%'" in cursor.asked
