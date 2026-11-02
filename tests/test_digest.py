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
        return []


def test_gold_rows_become_plain_values_whole_numbers_stay_whole():
    gold = digest.read_gold(GoldCursor())
    row = gold["kpis"][0]
    assert row["as_of_date"] == "2026-10-21"
    assert row["revenue_mtd_usd"] == 1000.5 and isinstance(row["revenue_mtd_usd"], float)
    assert row["net_subscriptions_l30d"] == 4 and isinstance(row["net_subscriptions_l30d"], int)
    digest.digest_message(digest.build_digest(gold))     # formats counts with :+d
