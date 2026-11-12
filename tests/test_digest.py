"""The executive digest's alert thresholds and message. build_digest is pure, so no warehouse is needed."""
from datetime import date
from decimal import Decimal

from crm_pipeline import digest


def kpi(brand="broomx", cancelled=0.12, mtd=1000.0, pmtd=900.0):
    return {"brand": brand, "brand_name": brand.title(), "as_of_date": "2026-10-21", "revenue_mtd_usd": mtd,
            "revenue_pmtd_usd": pmtd, "new_customers_mtd": 30, "net_subscriptions_l30d": 4, "canceled_within_30d_rate": cancelled}


def channel(name, ratio, brand="broomx"):
    return {"brand": brand, "brand_name": brand.title(), "channel": name, "spend_usd": 500.0,
            "acquired_customers": 10, "cac_usd": 50.0 if ratio is not None else None,
            "ltv_90d_usd": 60.0, "ltv_to_cac_ratio": ratio}


def anomaly(metric="revenue_usd", value=639.72, usual=279.91, direction="above", brand="broomx", day="2026-10-30"):
    """A row of exec_kpi_anomalies as read_gold returns it: the as-of day's, and flagged."""
    return {"brand": brand, "brand_name": brand.title(), "date_day": day, "metric": metric, "value": value,
            "baseline_median": usual, "robust_z": 5.81, "direction": direction}


def test_numbers_within_their_thresholds_raise_no_alert():
    result = digest.build_digest({"kpis": [kpi()], "channels": [channel("facebook", 1.6), channel("organic", None)]})
    assert result["alerts"] == []
    assert result["as_of_date"] == "2026-10-21"
    assert ":white_check_mark:" in digest.digest_message(result)


def test_each_threshold_breach_is_reported():
    result = digest.build_digest({
        "kpis": [kpi(cancelled=0.40, mtd=700.0, pmtd=1000.0)],
        "channels": [channel("tiktok", 0.8), channel("facebook", 1.6)],
    })
    assert len(result["alerts"]) == 3
    assert "40% of subscriptions cancelled within 30 days" in result["alerts"][0]
    assert "revenue month to date is -30%" in result["alerts"][1]
    assert "tiktok: 90-day LTV is 0.80x" in result["alerts"][2]
    message = digest.digest_message(result)
    assert message.index(":warning:") < message.index("*Broomx*: revenue MTD $700")
    assert "90-day LTV:CAC tiktok 0.8x, facebook 1.6x" in message


def test_one_unusual_number_adds_exactly_one_sentence_and_leads_the_message():
    quiet = digest.build_digest({"kpis": [kpi()], "channels": [channel("facebook", 1.6)], "anomalies": []})
    result = digest.build_digest({"kpis": [kpi()], "channels": [channel("facebook", 1.6)], "anomalies": [anomaly()]})
    assert result["anomalies"] == ["Broomx: revenue $639.72 on Friday 2026-10-30, above its usual Friday ($279.91)"]
    assert result["alerts"] == quiet["alerts"] == []            # unusual is not a threshold breach
    lines = digest.digest_message(result).splitlines()
    assert lines[1] == ":bar_chart: 1 unusual for the brand and the weekday:"
    assert lines[2] == "• Broomx: revenue $639.72 on Friday 2026-10-30, above its usual Friday ($279.91)"
    assert lines[3] == ":white_check_mark: Every number is within its alert threshold."
    # Nothing else in the message changed
    assert lines[3:] == digest.digest_message(quiet).splitlines()[1:]


def test_no_unusual_number_adds_no_sentence():
    for gold in ({"kpis": [kpi()], "channels": [], "anomalies": []}, {"kpis": [kpi()], "channels": []}):
        result = digest.build_digest(gold)
        assert result["anomalies"] == []
        assert ":bar_chart:" not in digest.digest_message(result)
        assert "unusual" not in digest.digest_message(result)


def test_the_three_threshold_alerts_are_the_same_beside_unusual_numbers():
    breaches = {"kpis": [kpi(cancelled=0.40, mtd=700.0, pmtd=1000.0)],
                "channels": [channel("tiktok", 0.8), channel("facebook", 1.6)]}
    with_unusual = digest.build_digest({**breaches, "anomalies": [anomaly(), anomaly("ad_spend_usd", 7.22, 19.68, "below")]})
    assert with_unusual["alerts"] == digest.build_digest(breaches)["alerts"]
    assert len(with_unusual["alerts"]) == 3 and len(with_unusual["anomalies"]) == 2
    message = digest.digest_message(with_unusual)
    assert message.index(":bar_chart: 2 unusual") < message.index(":warning: 3 past an alert threshold") \
        < message.index("*Broomx*: revenue MTD $700")


def test_a_sentence_names_the_metric_the_value_the_usual_value_and_the_side():
    assert digest.anomaly_sentence(anomaly("canceled_subscriptions", 3.0, 0.0, "above", "greenz", "2026-10-10")) == \
        "Greenz: cancelled subscriptions 3 on Saturday 2026-10-10, above its usual Saturday (0)"
    # A median of eight counts can fall between two of them
    assert digest.anomaly_sentence(anomaly("revenue_orders", 17.0, 6.5)) == \
        "Broomx: orders 17 on Friday 2026-10-30, above its usual Friday (6.5)"
    assert digest.anomaly_sentence(anomaly("contribution_margin_usd", -75.19, 113.36, "below", day="2026-09-30")) == \
        "Broomx: contribution margin -$75.19 on Wednesday 2026-09-30, below its usual Wednesday ($113.36)"
    # A metric added to the dbt var before it has a name here is still reported
    assert digest.anomaly_sentence(anomaly("refunds_usd", 80.0, 5.0)) == \
        "Broomx: refunds_usd 80 on Friday 2026-10-30, above its usual Friday (5)"


def test_every_metric_dbt_can_flag_has_a_name_in_the_digest():
    import yaml

    from crm_pipeline.config import DBT_PROJECT_DIR

    flagged = yaml.safe_load((DBT_PROJECT_DIR / "dbt_project.yml").read_text())["vars"]["anomaly_min_difference"]
    assert set(flagged) == set(digest.ANOMALY_METRICS)


def test_no_revenue_last_month_is_not_a_drop():
    assert digest.build_digest({"kpis": [kpi(mtd=50.0, pmtd=0.0)], "channels": []})["alerts"] == []


class GoldCursor:
    def __init__(self):
        self.queries = []

    def execute(self, sql):
        self.queries.append(sql)

    def fetchall(self):
        table = self.queries[-1].split(" from ")[1].split()[0]
        if table.endswith("EXEC_KPI_SNAPSHOT"):
            return [("broomx", "Broomx", date(2026, 10, 21), Decimal("1000.50"), Decimal("900.00"), 30, Decimal("4"), Decimal("0.125"))]
        if table.endswith("EXEC_KPI_ANOMALIES"):
            return [("broomx", "Broomx", date(2026, 10, 21), "revenue_orders", 17.0, 6.5, Decimal("6.74"), "above")]
        return []


def test_gold_rows_become_plain_values_whole_numbers_stay_whole():
    gold = digest.read_gold(GoldCursor())
    row = gold["kpis"][0]
    assert row["as_of_date"] == "2026-10-21"
    assert row["revenue_mtd_usd"] == 1000.5 and isinstance(row["revenue_mtd_usd"], float)
    assert row["net_subscriptions_l30d"] == 4 and isinstance(row["net_subscriptions_l30d"], int)
    digest.digest_message(digest.build_digest(gold))     # formats counts with :+d


def test_only_the_as_of_days_flagged_rows_are_read_and_they_become_a_sentence():
    cursor = GoldCursor()
    gold = digest.read_gold(cursor)
    query = next(sql for sql in cursor.queries if "EXEC_KPI_ANOMALIES" in sql)
    assert " where is_as_of_day and is_anomaly order by brand, metric" in query
    assert not [sql for sql in cursor.queries if "EXEC_KPI_ANOMALIES" not in sql and " where " in sql]
    assert gold["anomalies"] == [{"brand": "broomx", "brand_name": "Broomx", "date_day": "2026-10-21",
                                  "metric": "revenue_orders", "value": 17.0, "baseline_median": 6.5,
                                  "robust_z": 6.74, "direction": "above"}]
    result = digest.build_digest(gold)
    assert result["anomalies"] == ["Broomx: orders 17 on Wednesday 2026-10-21, above its usual Wednesday (6.5)"]
    import json
    json.dumps(result)          # Airflow carries the digest between two tasks as JSON
