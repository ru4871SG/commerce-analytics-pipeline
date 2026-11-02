"""The executive digest: each brand's headline numbers, and the ones past an alert threshold.
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal

from crm_pipeline.config import ANALYTICS_DATABASE, GOLD_SCHEMA

# Alert thresholds. A breach leads the digest but does not fail the DAG: the numbers are right,
# the news is bad.
CANCELLED_30D_ABOVE = 0.35        # share of first subscriptions cancelled within 30 days
LTV_TO_CAC_BELOW = 1.0            # 90-day margin per customer a paid channel brought, over what it paid
REVENUE_MTD_DROP_ABOVE = 0.20     # revenue month to date, against the same days of last month

KPI_COLUMNS = ("brand", "brand_name", "as_of_date", "revenue_mtd_usd", "revenue_pmtd_usd",
               "new_customers_mtd", "net_subscriptions_l30d", "canceled_within_30d_rate")
CHANNEL_COLUMNS = ("brand", "brand_name", "channel", "spend_usd", "acquired_customers", "cac_usd",
                   "ltv_90d_usd", "ltv_to_cac_ratio")


def _plain(value):
    """A warehouse value as one XCom can carry. Whole numbers stay whole, for counts."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def read_gold(cursor) -> dict[str, list[dict]]:
    """The KPI cards and the channel CAC / LTV rows, one dict per row."""
    def rows(table: str, columns: tuple[str, ...], order_by: str) -> list[dict]:
        cursor.execute(f"select {', '.join(columns)} from {ANALYTICS_DATABASE}.{GOLD_SCHEMA}.{table} order by {order_by}")
        return [dict(zip(columns, map(_plain, row))) for row in cursor.fetchall()]

    return {
        "kpis": rows("EXEC_KPI_SNAPSHOT", KPI_COLUMNS, "brand"),
        "channels": rows("EXEC_CHANNEL_CAC_LTV", CHANNEL_COLUMNS, "brand, channel"),
    }


def _change(now: float, before: float) -> float | None:
    return now / before - 1 if before else None


def build_digest(gold: dict[str, list[dict]]) -> dict:
    """The digest of read_gold's rows: the rows themselves and a sentence per threshold breach."""
    kpis, channels = gold["kpis"], gold["channels"]
    alerts = []
    for kpi in kpis:
        cancelled = kpi["canceled_within_30d_rate"]
        if cancelled is not None and cancelled > CANCELLED_30D_ABOVE:
            alerts.append(f"{kpi['brand_name']}: {cancelled:.0%} of subscriptions cancelled within 30 days "
                          f"(alert above {CANCELLED_30D_ABOVE:.0%})")
        change = _change(kpi["revenue_mtd_usd"], kpi["revenue_pmtd_usd"])
        if change is not None and change < -REVENUE_MTD_DROP_ABOVE:
            alerts.append(f"{kpi['brand_name']}: revenue month to date is {change:.0%} on the same days "
                          f"last month (alert below -{REVENUE_MTD_DROP_ABOVE:.0%})")
    for channel in channels:
        ratio = channel["ltv_to_cac_ratio"]
        if ratio is not None and ratio < LTV_TO_CAC_BELOW:
            alerts.append(f"{channel['brand_name']}, {channel['channel']}: 90-day LTV is {ratio:.2f}x the "
                          f"${channel['cac_usd']:,.2f} it costs to acquire a customer (alert below {LTV_TO_CAC_BELOW:.1f}x)")
    return {
        "as_of_date": max((kpi["as_of_date"] for kpi in kpis), default=None),
        "kpis": kpis,
        "channels": channels,
        "alerts": alerts,
    }


def digest_message(digest: dict) -> str:
    """The digest as a Slack message: breaches first, then one line per brand."""
    lines = [f"*Executive report, {digest['as_of_date']}*"]
    if digest["alerts"]:
        lines.append(f":warning: {len(digest['alerts'])} past an alert threshold:")
        lines += [f"• {alert}" for alert in digest["alerts"]]
    else:
        lines.append(":white_check_mark: Every number is within its alert threshold.")
    for kpi in digest["kpis"]:
        change = _change(kpi["revenue_mtd_usd"], kpi["revenue_pmtd_usd"])
        cancelled = kpi["canceled_within_30d_rate"]
        channels = ", ".join(
            f"{c['channel']} {c['ltv_to_cac_ratio']:.1f}x" for c in digest["channels"]
            if c["brand"] == kpi["brand"] and c["ltv_to_cac_ratio"] is not None
        )
        lines.append(
            f"*{kpi['brand_name']}*: revenue MTD ${kpi['revenue_mtd_usd']:,.0f}"
            f"{f' ({change:+.0%} on last month)' if change is not None else ''}, "
            f"{kpi['new_customers_mtd']} new customers, net subscriptions L30D {kpi['net_subscriptions_l30d']:+d}, "
            f"cancelled within 30 days {'n/a' if cancelled is None else f'{cancelled:.0%}'}"
            f"{f'; 90-day LTV:CAC {channels}' if channels else ''}"
        )
    return "\n".join(lines)
