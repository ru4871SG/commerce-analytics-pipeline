#!/usr/bin/env python3
"""Build local/superset/executive_report.zip, a Superset 6 import bundle.

Run with the Superset virtualenv (it needs PyYAML, which Superset already depends on):

    .venv-superset/bin/python local/superset/build_bundle.py

Stable UUIDs make the import idempotent: importing again updates the same database,
datasets, charts and dashboard instead of creating duplicates.
"""
from __future__ import annotations

import shutil
import uuid
import zipfile
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
OUT = HERE / "executive_report.zip"
BUNDLE = HERE / "bundle"

NAMESPACE = uuid.UUID("f3c1a2d4-6b7e-4c8a-9d10-2e3f40516273")
DATABASE_UUID = str(uuid.uuid5(NAMESPACE, "database"))
DASHBOARD_UUID = str(uuid.uuid5(NAMESPACE, "dashboard/executive-report"))
SLUG = "executive-report"
VERSION = "1.0.0"
TIMESTAMP = "2026-10-22T00:00:00+00:00"
SQLALCHEMY_URI = (
    "snowflake://fake:XXXXXXXXXX@fakesnow/ANALYTICS/GOLD"
    "?warehouse=TRANSFORMING_WH&role=TRANSFORMER"
)


def uid(kind: str, name: str) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{kind}/{name}"))


# ---------------------------------------------------------------------------
# Datasets. Types are metadata for the Superset UI; queries go to Snowflake.
# ---------------------------------------------------------------------------
def col(name: str, type_: str = "VARCHAR", is_dttm: bool = False, label: str | None = None) -> dict:
    return {
        "column_name": name,
        "extra": None,
        "verbose_name": label,      # the heading a table shows instead of the column's name
        "is_dttm": is_dttm,
        "is_active": True,
        "type": type_,
        "advanced_data_type": None,
        "groupby": True,
        "filterable": True,
        "expression": None,
        "description": None,
        "python_date_format": None,
        "datetime_format": None,
    }


def metric(name: str, expression: str, verbose_name: str, d3format: str | None = None) -> dict:
    return {
        "metric_name": name,
        "verbose_name": verbose_name,
        "metric_type": None,
        "expression": expression,
        "description": None,
        "d3format": d3format,
        "currency": None,
        "extra": None,
        "warning_text": None,
    }


DATASETS = {
    "exec_daily_brand_metrics": {
        "columns": [
            col("brand"), col("brand_name"), col("date_day", "TIMESTAMP", True),
            col("gross_sales_usd", "DOUBLE"), col("refunds_usd", "DOUBLE"), col("revenue_usd", "DOUBLE"),
            col("contribution_margin_usd", "DOUBLE"), col("revenue_orders", "BIGINT"),
            col("new_customers", "BIGINT"), col("new_subscriptions", "BIGINT"),
            col("canceled_subscriptions", "BIGINT"), col("net_subscriptions", "BIGINT"),
            col("ad_spend_usd", "DOUBLE"),
        ],
        "main_dttm_col": "date_day",
        "description": "Brand x day metrics behind the weekly revenue, contribution margin, new-customer, ad spend and monthly subscription charts.",
        "metrics": [
            metric("sum__revenue_usd", "SUM(revenue_usd)", "Revenue (USD)", "$,.2f"),
            metric("sum__contribution_margin_usd", "SUM(contribution_margin_usd)", "Contribution margin (USD)", "$,.2f"),
            metric("sum__new_customers", "SUM(new_customers)", "New customers"),
            metric("sum__new_subscriptions", "SUM(new_subscriptions)", "New subscriptions"),
            metric("sum__canceled_subscriptions", "SUM(canceled_subscriptions)", "Cancelled subscriptions"),
            metric("sum__net_subscriptions", "SUM(net_subscriptions)", "Net subscriptions"),
            metric("sum__ad_spend_usd", "SUM(ad_spend_usd)", "Ad spend (USD)", "$,.2f"),
        ],
    },
    "exec_kpi_snapshot": {
        "columns": [
            col("brand"), col("brand_name"), col("as_of_date", "TIMESTAMP", True),
            *(col(name, "DOUBLE") for name in (
                "revenue_mtd_usd", "revenue_pmtd_usd", "revenue_ytd_usd", "revenue_pytd_usd",
                "contribution_margin_mtd_usd", "contribution_margin_pmtd_usd",
                "contribution_margin_ytd_usd", "contribution_margin_pytd_usd",
            )),
            *(col(name, "BIGINT") for name in (
                "new_customers_mtd", "new_customers_pmtd", "new_customers_l30d",
                "new_customers_l60d", "new_customers_l90d",
                "net_subscriptions_l30d", "net_subscriptions_l60d",
                "net_subscriptions_l90d", "net_subscriptions_l180d",
                "active_subscriptions_start_l30d", "lost_subscriptions_l30d",
                "active_subscriptions_start_l60d", "lost_subscriptions_l60d",
                "active_subscriptions_start_l90d", "lost_subscriptions_l90d",
            )),
            *(col(name, "DOUBLE") for name in (
                "churn_rate_l30d", "churn_rate_l60d", "churn_rate_l90d",
                "canceled_within_30d_rate", "canceled_within_60d_rate", "canceled_within_90d_rate",
            )),
        ],
        "main_dttm_col": "as_of_date",
        "description": "One row per brand with every number card, measured at the as-of date.",
        "metrics": [
            metric("sum__revenue_mtd_usd", "SUM(revenue_mtd_usd)", "Revenue MTD (USD)", "$,.2f"),
            metric("sum__revenue_pmtd_usd", "SUM(revenue_pmtd_usd)", "Revenue PMTD (USD)", "$,.2f"),
            metric("sum__revenue_ytd_usd", "SUM(revenue_ytd_usd)", "Revenue YTD (USD)", "$,.2f"),
            metric("sum__revenue_pytd_usd", "SUM(revenue_pytd_usd)", "Revenue PYTD (USD)", "$,.2f"),
            metric("sum__contribution_margin_mtd_usd", "SUM(contribution_margin_mtd_usd)", "Contribution margin MTD (USD)", "$,.2f"),
            metric("sum__contribution_margin_pmtd_usd", "SUM(contribution_margin_pmtd_usd)", "Contribution margin PMTD (USD)", "$,.2f"),
            metric("sum__contribution_margin_ytd_usd", "SUM(contribution_margin_ytd_usd)", "Contribution margin YTD (USD)", "$,.2f"),
            metric("sum__contribution_margin_pytd_usd", "SUM(contribution_margin_pytd_usd)", "Contribution margin PYTD (USD)", "$,.2f"),
            metric("sum__new_customers_mtd", "SUM(new_customers_mtd)", "Customers MTD"),
            metric("sum__new_customers_pmtd", "SUM(new_customers_pmtd)", "Customers PMTD"),
            metric("sum__new_customers_l30d", "SUM(new_customers_l30d)", "Customers L30D"),
            metric("sum__new_customers_l60d", "SUM(new_customers_l60d)", "Customers L60D"),
            metric("sum__new_customers_l90d", "SUM(new_customers_l90d)", "Customers L90D"),
            metric("sum__net_subscriptions_l30d", "SUM(net_subscriptions_l30d)", "Net subscriptions L30D"),
            metric("sum__net_subscriptions_l60d", "SUM(net_subscriptions_l60d)", "Net subscriptions L60D"),
            metric("sum__net_subscriptions_l90d", "SUM(net_subscriptions_l90d)", "Net subscriptions L90D"),
            metric("sum__net_subscriptions_l180d", "SUM(net_subscriptions_l180d)", "Net subscriptions L180D"),
            metric("churn_l30d", "SUM(lost_subscriptions_l30d) / NULLIF(SUM(active_subscriptions_start_l30d), 0)", "Churn L30D", ".1%"),
            metric("churn_l60d", "SUM(lost_subscriptions_l60d) / NULLIF(SUM(active_subscriptions_start_l60d), 0)", "Churn L60D", ".1%"),
            metric("churn_l90d", "SUM(lost_subscriptions_l90d) / NULLIF(SUM(active_subscriptions_start_l90d), 0)", "Churn L90D", ".1%"),
            metric("avg__canceled_within_30d_rate", "AVG(canceled_within_30d_rate)", "Cancelled within 30 days", ".1%"),
            metric("avg__canceled_within_60d_rate", "AVG(canceled_within_60d_rate)", "Cancelled within 60 days", ".1%"),
            metric("avg__canceled_within_90d_rate", "AVG(canceled_within_90d_rate)", "Cancelled within 90 days", ".1%"),
            metric("max__as_of_date", "MAX(as_of_date)", "As-of date"),
        ],
    },
    "exec_subscription_survival": {
        "columns": [
            col("brand"), col("brand_name"), col("day_offset", "BIGINT"),
            col("subscriptions", "BIGINT"), col("active_subscriptions", "BIGINT"),
            col("subscribers_left_rate", "DOUBLE"), col("canceled_rate", "DOUBLE"),
        ],
        "main_dttm_col": None,
        "description": "Share of subscriptions at least N days old still active N days after they started.",
        "metrics": [
            metric("max__subscribers_left_rate", "MAX(subscribers_left_rate)", "Subscribers left", ".1%"),
            metric("max__canceled_rate", "MAX(canceled_rate)", "Cancelled", ".1%"),
        ],
    },
    "exec_rebuy_curve": {
        "columns": [
            col("brand"), col("brand_name"), col("day_offset", "BIGINT"),
            col("rebuyers", "BIGINT"), col("customers", "BIGINT"), col("rebuy_rate", "DOUBLE"),
        ],
        "main_dttm_col": None,
        "description": "Share of customers who ordered again within N days of their first order.",
        "metrics": [
            metric("max__rebuy_rate", "MAX(rebuy_rate)", "Rebuy rate", ".1%"),
        ],
    },
    "exec_active_subscriptions_weekly": {
        "columns": [
            col("brand"), col("brand_name"), col("week_start", "TIMESTAMP", True),
            col("snapshot_date", "TIMESTAMP", True), col("active_subscriptions", "BIGINT"),
        ],
        "main_dttm_col": "week_start",
        "description": "Active subscriptions per brand at the end of each Monday-start week.",
        "metrics": [
            metric("sum__active_subscriptions", "SUM(active_subscriptions)", "Active subscriptions"),
        ],
    },
    "exec_daily_orders_live": {
        "columns": [
            col("brand"), col("brand_name"), col("date_day", "TIMESTAMP", True),
            col("orders", "BIGINT"), col("paid_orders", "BIGINT"), col("new_subscriptions", "BIGINT"),
            col("renewals", "BIGINT"), col("sales_usd", "DOUBLE"),
        ],
        "main_dttm_col": "date_day",
        "description": "Orders and sales per brand and day, over a Snowflake dynamic table that refreshes itself.",
        "metrics": [
            metric("sum__orders", "SUM(orders)", "Orders"),
            metric("sum__sales_usd", "SUM(sales_usd)", "Sales (USD)", "$,.2f"),
        ],
    },
    "exec_cohort_contribution_margin": {
        "columns": [
            col("brand"), col("brand_name"), col("first_order_month", "TIMESTAMP", True),
            col("cohort_number", "BIGINT"), col("new_customers", "BIGINT"),
            col("period_contribution_margin_usd", "DOUBLE"), col("cumulative_contribution_margin_usd", "DOUBLE"),
            col("contribution_margin_per_customer_usd", "DOUBLE"),
        ],
        "main_dttm_col": "first_order_month",
        "description": "Cumulative contribution margin per customer, by first-order month and 30-day period.",
        "metrics": [
            metric("avg__contribution_margin_per_customer_usd", "AVG(contribution_margin_per_customer_usd)", "Contribution margin per customer (USD)", "$,.2f"),
        ],
    },
    "exec_channel_cac_ltv": {
        "columns": [
            col("brand"), col("brand_name"), col("channel"),
            col("acquisition_window_start", "TIMESTAMP", True), col("acquisition_window_end", "TIMESTAMP", True),
            col("spend_usd", "DOUBLE"), col("impressions", "BIGINT"), col("clicks", "BIGINT"),
            col("acquired_customers", "BIGINT"), col("contribution_margin_90d_usd", "DOUBLE"),
            col("cac_usd", "DOUBLE"), col("ltv_90d_usd", "DOUBLE"), col("ltv_to_cac_ratio", "DOUBLE"),
        ],
        "main_dttm_col": None,
        "description": "Customer acquisition cost and 90-day LTV per brand and channel.",
        "metrics": [
            metric("avg__cac_usd", "AVG(cac_usd)", "CAC (USD)", "$,.2f"),
            metric("avg__ltv_90d_usd", "AVG(ltv_90d_usd)", "90-day LTV (USD)", "$,.2f"),
            metric("max__ltv_to_cac_ratio", "MAX(ltv_to_cac_ratio)", "90-day LTV : CAC", ",.2f"),
        ],
    },
    "exec_web_daily_sessions": {
        "columns": [
            col("brand"), col("brand_name"), col("date_day", "TIMESTAMP", True), col("channel"),
            col("sessions", "BIGINT"), col("engaged_sessions", "BIGINT"), col("view_item_sessions", "BIGINT"),
            col("add_to_cart_sessions", "BIGINT"), col("checkout_sessions", "BIGINT"),
            col("purchase_sessions", "BIGINT"), col("purchases", "BIGINT"), col("ad_spend_usd", "DOUBLE"),
        ],
        "main_dttm_col": "date_day",
        "description": "Storefront sessions per brand, day and channel, by funnel step, from the lakehouse.",
        "metrics": [
            metric("sum__sessions", "SUM(sessions)", "Sessions"),
        ],
    },
    "exec_web_channel_funnel": {
        "columns": [
            col("brand"), col("brand_name"), col("channel"),
            col("window_start", "TIMESTAMP", True), col("window_end", "TIMESTAMP", True),
            col("sessions", "BIGINT"), col("purchases", "BIGINT"), col("ad_spend_usd", "DOUBLE"),
            col("engaged_rate", "DOUBLE"), col("view_item_rate", "DOUBLE"), col("add_to_cart_rate", "DOUBLE"),
            col("checkout_rate", "DOUBLE"), col("conversion_rate", "DOUBLE"),
            col("cost_per_session_usd", "DOUBLE"), col("cost_per_purchase_usd", "DOUBLE"),
        ],
        "main_dttm_col": None,
        "description": "The storefront funnel per brand and channel over the last 90 days.",
        "metrics": [
            metric("avg__add_to_cart_rate", "AVG(add_to_cart_rate)", "Add to cart", ".2%"),
            metric("avg__checkout_rate", "AVG(checkout_rate)", "Checkout", ".2%"),
            metric("avg__conversion_rate", "AVG(conversion_rate)", "Purchase", ".2%"),
        ],
    },
    "exec_kpi_anomalies": {
        "columns": [
            col("brand"), col("brand_name", label="Brand"), col("date_day", "TIMESTAMP", True), col("metric", label="Metric"),
            col("value", "DOUBLE", label="On the day"), col("baseline_median", "DOUBLE", label="Usual for the weekday"),
            col("spread", "DOUBLE"), col("robust_z", "DOUBLE", label="Robust z-score"),
            col("direction", label="Side"), col("is_anomaly", "BOOLEAN", label="Unusual"),
            col("is_as_of_day", "BOOLEAN"), col("comparison_days", "BIGINT", label="Earlier weeks compared"),
        ],
        "main_dttm_col": "date_day",
        "description": "Each brand's daily numbers against the same weekday of the eight weeks before, for the last 90 days.",
        "metrics": [],      # its chart shows the rows as they are stored
    },
    "exec_product_performance": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True),
            col("product_name"), col("purchase_type"), col("units", "BIGINT"),
            col("line_revenue_usd", "DOUBLE"), col("line_cost_usd", "DOUBLE"), col("line_margin_usd", "DOUBLE"),
            col("units_in_refunded_orders", "BIGINT"),
        ],
        "main_dttm_col": "month_start",
        "description": "Units, line revenue, cost and margin per brand, month, product and purchase type, on revenue orders.",
        "metrics": [
            metric("sum__line_margin_usd", "SUM(line_margin_usd)", "Line margin (USD)", "$,.2f"),
            metric("sum__units", "SUM(units)", "Units"),
        ],
    },
    "exec_refund_reasons": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True), col("refund_reason"),
            col("refunds", "BIGINT"), col("refund_amount_usd", "DOUBLE"),
            col("refunds_requiring_return", "BIGINT"), col("days_to_refund_sum", "BIGINT"),
        ],
        "main_dttm_col": "month_start",
        "description": "The refunds that come off revenue, per brand, month of issue and reason.",
        "metrics": [
            metric("sum__refund_amount_usd", "SUM(refund_amount_usd)", "Refunded (USD)", "$,.2f"),
            metric("avg_days_to_refund", "SUM(days_to_refund_sum) / NULLIF(SUM(refunds), 0)", "Average days to refund", ",.1f"),
        ],
    },
    "exec_upsell_attach": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True),
            col("first_orders", "BIGINT"), col("first_orders_with_upsell", "BIGINT"),
        ],
        "main_dttm_col": "month_start",
        "description": "Customers' first orders per brand and month, and how many of them have an upsell line.",
        "metrics": [
            metric("upsell_attach_rate", "SUM(first_orders_with_upsell) / NULLIF(SUM(first_orders), 0)", "Upsell attach rate", ".1%"),
        ],
    },
    "exec_mrr_movements": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True), col("movement_type"),
            col("mrr_usd", "DOUBLE"), col("subscriptions", "BIGINT"),
        ],
        "main_dttm_col": "month_start",
        "description": "What moved monthly recurring revenue per brand and month: new, reactivated and churned (negative) subscriptions.",
        "metrics": [
            # The same sum twice: by movement type it is one movement, over all types the net change.
            # The mixed chart's legend writes the first as "MRR (USD), new", so its name is short
            metric("sum__mrr_usd", "SUM(mrr_usd)", "MRR (USD)", "$,.2f"),
            metric("net_new_mrr_usd", "SUM(mrr_usd)", "Net new MRR (USD)", "$,.2f"),
        ],
    },
    "exec_mrr_monthly": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True),
            *(col(name, "DOUBLE") for name in (
                "opening_mrr_usd", "new_mrr_usd", "reactivated_mrr_usd", "churned_mrr_usd",
                "closing_mrr_usd", "closing_paused_mrr_usd",
            )),
            *(col(name, "BIGINT") for name in (
                "opening_subscriptions", "new_subscriptions", "reactivated_subscriptions",
                "churned_subscriptions", "closing_subscriptions", "closing_paused_subscriptions",
            )),
            col("is_complete_month", "BOOLEAN"),
        ],
        "main_dttm_col": "month_start",
        "description": "Monthly recurring revenue per brand and month as a roll-forward: opening, new, reactivated, churned, closing.",
        "metrics": [
            metric("sum__closing_mrr_usd", "SUM(closing_mrr_usd)", "MRR (USD)", "$,.2f"),
            metric("revenue_churn_rate", "SUM(churned_mrr_usd) / NULLIF(SUM(opening_mrr_usd), 0)", "Revenue churn", ".1%"),
        ],
    },
    "exec_fx_impact": {
        "columns": [
            col("brand"), col("brand_name"), col("month_start", "TIMESTAMP", True),
            col("revenue_usd_constant", "DOUBLE"), col("revenue_usd_actual", "DOUBLE"), col("fx_effect_usd", "DOUBLE"),
        ],
        "main_dttm_col": "month_start",
        "description": "Revenue per brand and month at the constant rate and at the ECB's rate of each day, and the difference.",
        "metrics": [
            metric("sum__revenue_usd_actual", "SUM(revenue_usd_actual)", "At the rates of the day (USD)", "$,.2f"),
            metric("sum__revenue_usd_constant", "SUM(revenue_usd_constant)", "At constant rates (USD)", "$,.2f"),
            metric("sum__fx_effect_usd", "SUM(fx_effect_usd)", "Exchange-rate effect (USD)", "$,.2f"),
        ],
    },
}
for name, definition in DATASETS.items():
    definition["uuid"] = uid("dataset", name)


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
BASE = {
    "datasource": "0__table",
    "extra_form_data": {},
    "adhoc_filters": [],
    "time_range": "No filter",
    "annotation_layers": [],
    "dashboards": [],
}
# What a mixed_timeseries chart takes as seriesType and seriesTypeB: an ECharts series type, or
# one of three step lines (Superset's EchartsTimeseriesSeriesType). Anything else, the name of a
# chart plugin included, is not an error: the query is drawn as lines.
MIXED_SERIES_TYPES = ("line", "scatter", "smooth", "bar", "start", "middle", "end")


def chart(name: str, viz_type: str, dataset: str, params: dict, description: str = "") -> dict:
    return {
        "slice_name": name,
        "description": description or None,
        "certified_by": None,
        "certification_details": None,
        "viz_type": viz_type,
        "params": {**BASE, **params, "viz_type": viz_type},
        "query_context": None,
        "cache_timeout": None,
        "uuid": uid("chart", name),
        "version": VERSION,
        "dataset_uuid": DATASETS[dataset]["uuid"],
    }


def big_number(name: str, dataset: str, metric_name: str, fmt: str, description: str = "",
               time_format: str = "smart_date") -> dict:
    return chart(name, "big_number_total", dataset, {
        "metric": metric_name,
        "y_axis_format": fmt,
        "subheader": "",
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "time_format": time_format,
    }, description)


def latest_day(name: str, dataset: str, metric_name: str, fmt: str, description: str = "") -> dict:
    """A Big Number for the latest day, with the daily trend underneath and the change on the day before."""
    return chart(name, "big_number", dataset, {
        "metric": metric_name,
        "x_axis": "date_day",
        "time_grain_sqla": "P1D",
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "compare_lag": 1,
        "compare_suffix": "vs the day before",
        "y_axis_format": fmt,
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "time_format": "smart_date",
        "rolling_type": "None",
    }, description)


def sql_filter(expression: str, name: str) -> dict:
    """A chart's own filter, written in SQL. check_dashboard.py applies it when it queries the chart."""
    return {
        "clause": "WHERE", "expressionType": "SQL", "sqlExpression": expression,
        "subject": None, "operator": None, "comparator": None, "isExtra": False, "isNew": False,
        "filterOptionName": name,
    }


def latest_complete_month(name: str, dataset: str, metric_name: str, fmt: str, description: str = "") -> dict:
    """A Big Number for the latest month that is over, named under it, with the months before as a trend."""
    return chart(name, "big_number", dataset, {
        "metric": metric_name,
        "x_axis": "month_start",
        "time_grain_sqla": "P1M",
        # The month that holds the as-of date is not over: its number would be a part of a month's
        "adhoc_filters": [sql_filter("is_complete_month = true", "filter_complete_months")],
        "show_trend_line": True,
        "start_y_axis_at_zero": True,
        "show_timestamp": True,
        "y_axis_format": fmt,
        "header_font_size": 0.4,
        "subheader_font_size": 0.15,
        "time_format": "%B %Y",
        "rolling_type": "None",
    }, description)


def time_series(name: str, dataset: str, viz_type: str, metrics: list[str],
                x_axis: str = "date_day", grain: str = "P1W", groupby: bool = True,
                stack: bool = False, fmt: str = "SMART_NUMBER", description: str = "",
                dimension: str = "brand_name", horizontal: bool = False) -> dict:
    return chart(name, viz_type, dataset, {
        # Bars lying down, for an axis of names: standing up, every second name is left out for room
        **({"orientation": "horizontal"} if horizontal else {}),
        "x_axis": x_axis,
        "time_grain_sqla": grain,
        "metrics": metrics,
        "groupby": [dimension] if groupby else [],
        "row_limit": 10000,
        "order_desc": True,
        "show_legend": True,
        "rich_tooltip": True,
        "legendOrientation": "top",
        "legendType": "scroll",
        "x_axis_time_format": "smart_date",
        "x_axis_sort_asc": True,
        "y_axis_format": fmt,
        "y_axis_bounds": [None, None],
        "truncateXAxis": True,
        "truncate_metric": True,
        "show_empty_columns": True,
        "seriesType": viz_type.replace("echarts_timeseries_", ""),
        "stack": "Stack" if stack else None,
        "color_scheme": "supersetColors",
    }, description)


CHARTS = [
    big_number("Revenue MTD", "exec_kpi_snapshot", "sum__revenue_mtd_usd", "$,.2f",
               "Order totals from the first of the month to the as-of date, VAT removed, minus the refunds "
               "issued in those days, in USD at constant-currency rates. Before the cost of the orders."),
    big_number("Revenue PMTD", "exec_kpi_snapshot", "sum__revenue_pmtd_usd", "$,.2f",
               "The same month-to-date span one month earlier."),
    big_number("Revenue YTD", "exec_kpi_snapshot", "sum__revenue_ytd_usd", "$,.2f"),
    big_number("Revenue PYTD", "exec_kpi_snapshot", "sum__revenue_pytd_usd", "$,.2f"),
    big_number("Contribution Margin MTD", "exec_kpi_snapshot", "sum__contribution_margin_mtd_usd", "$,.2f",
               "Revenue minus the cost of the orders (goods, shipping, processing, payment fees). "
               "Before ad spend."),
    big_number("Contribution Margin PMTD", "exec_kpi_snapshot", "sum__contribution_margin_pmtd_usd", "$,.2f",
               "The same month-to-date span one month earlier."),
    big_number("Contribution Margin YTD", "exec_kpi_snapshot", "sum__contribution_margin_ytd_usd", "$,.2f"),
    big_number("Contribution Margin PYTD", "exec_kpi_snapshot", "sum__contribution_margin_pytd_usd", "$,.2f"),
    big_number("Customers MTD", "exec_kpi_snapshot", "sum__new_customers_mtd", "SMART_NUMBER"),
    big_number("Customers PMTD", "exec_kpi_snapshot", "sum__new_customers_pmtd", "SMART_NUMBER"),
    big_number("Churn L30D", "exec_kpi_snapshot", "churn_l30d", ".1%",
               "Subscribers lost in the last 30 days, divided by those active 30 days ago."),
    big_number("Churn L60D", "exec_kpi_snapshot", "churn_l60d", ".1%",
               "Subscribers lost in the last 60 days, divided by those active 60 days ago."),
    big_number("Churn L90D", "exec_kpi_snapshot", "churn_l90d", ".1%",
               "Subscribers lost in the last 90 days, divided by those active 90 days ago."),
    big_number("Cancelled within 30 days", "exec_kpi_snapshot", "avg__canceled_within_30d_rate", ".1%",
               "Of the subscriptions at least 30 days old, the share cancelled in their first 30 days. "
               "Average of the brands' rates."),
    big_number("Cancelled within 60 days", "exec_kpi_snapshot", "avg__canceled_within_60d_rate", ".1%",
               "Of the subscriptions at least 60 days old, the share cancelled in their first 60 days. "
               "Average of the brands' rates."),
    big_number("Cancelled within 90 days", "exec_kpi_snapshot", "avg__canceled_within_90d_rate", ".1%",
               "Of the subscriptions at least 90 days old, the share cancelled in their first 90 days. "
               "Average of the brands' rates."),
    big_number("Customers L30D", "exec_kpi_snapshot", "sum__new_customers_l30d", "SMART_NUMBER"),
    big_number("Customers L60D", "exec_kpi_snapshot", "sum__new_customers_l60d", "SMART_NUMBER"),
    big_number("Customers L90D", "exec_kpi_snapshot", "sum__new_customers_l90d", "SMART_NUMBER"),
    big_number("Net Subscriptions L30D", "exec_kpi_snapshot", "sum__net_subscriptions_l30d", "SMART_NUMBER"),
    big_number("Net Subscriptions L60D", "exec_kpi_snapshot", "sum__net_subscriptions_l60d", "SMART_NUMBER"),
    big_number("Net Subscriptions L90D", "exec_kpi_snapshot", "sum__net_subscriptions_l90d", "SMART_NUMBER"),
    big_number("Net Subscriptions L180D", "exec_kpi_snapshot", "sum__net_subscriptions_l180d", "SMART_NUMBER"),
    latest_day("Orders, latest day", "exec_daily_orders_live", "sum__orders", "SMART_NUMBER",
               "Orders on the latest day with data. Refreshes as soon as the merge task applies new changes."),
    latest_day("Sales, latest day", "exec_daily_orders_live", "sum__sales_usd", "$,.2f",
               "Paid order totals without VAT, in USD, before refunds and the cost of the order."),
    # smart_date would show only "November" on the 1st of a month, so the card spells out the day
    big_number("As-of date", "exec_kpi_snapshot", "max__as_of_date", "smart_date",
               "Every MTD and last-N-days figure is measured from this date.", time_format="%b %-d, %Y"),
    time_series("Revenue by week", "exec_daily_brand_metrics", "echarts_timeseries_bar",
                ["sum__revenue_usd"], grain="P1W", stack=True, fmt="$,.2f"),
    time_series("Contribution margin by week", "exec_daily_brand_metrics", "echarts_timeseries_bar",
                ["sum__contribution_margin_usd"], grain="P1W", stack=True, fmt="$,.2f"),
    time_series("New customers by week", "exec_daily_brand_metrics", "echarts_timeseries_line",
                ["sum__new_customers"], grain="P1W"),
    time_series("Ad spend by week", "exec_daily_brand_metrics", "echarts_timeseries_bar",
                ["sum__ad_spend_usd"], grain="P1W", stack=True, fmt="$,.2f",
                description="Ad spend on every paid channel, in USD at constant-currency rates."),
    time_series("CAC and 90-day LTV by channel", "exec_channel_cac_ltv", "echarts_timeseries_bar",
                ["avg__cac_usd", "avg__ltv_90d_usd"], x_axis="channel", grain=None, groupby=False, fmt="$,.2f",
                description="What a customer cost to acquire on each channel, and the contribution margin of their "
                            "first 90 days. With several brands selected, the average of the brands' values."),
    time_series("90-day LTV : CAC by channel", "exec_channel_cac_ltv", "echarts_timeseries_bar",
                ["max__ltv_to_cac_ratio"], x_axis="channel", grain=None, fmt=",.2f",
                description="How many times a customer's first 90 days paid back what they cost. Below 1, the "
                            "channel has not paid for itself within 90 days."),
    time_series("Storefront sessions by channel", "exec_web_daily_sessions", "echarts_timeseries_bar",
                ["sum__sessions"], grain="P1W", stack=True, dimension="channel",
                description="Sessions on the shops' websites per week, by the channel they arrived from, from the "
                            "clickstream the lakehouse processes (pandas or Spark, whichever the batch needed)."),
    time_series("Storefront funnel by channel", "exec_web_channel_funnel", "echarts_timeseries_bar",
                ["avg__add_to_cart_rate", "avg__checkout_rate", "avg__conversion_rate"], x_axis="channel",
                grain=None, groupby=False, fmt=".2%",
                description="Of the last 90 days' sessions from each channel, the share that added to cart, began "
                            "checkout and purchased. With several brands selected, the average of the brands' rates."),
    time_series("Subscribers left after N days", "exec_subscription_survival", "echarts_timeseries_line",
                ["max__subscribers_left_rate"], x_axis="day_offset", grain=None, fmt=".1%",
                description="Share of subscriptions still active N days after they started. Only subscriptions at "
                            "least N days old count at day N."),
    time_series("Rebuy %", "exec_rebuy_curve", "echarts_timeseries_line",
                ["max__rebuy_rate"], x_axis="day_offset", grain=None, fmt=".1%"),
    time_series("Active subscriptions", "exec_active_subscriptions_weekly", "echarts_timeseries_line",
                ["sum__active_subscriptions"], x_axis="week_start", grain=None, fmt="SMART_NUMBER"),
    chart("Contribution margin per customer by cohort", "pivot_table_v2", "exec_cohort_contribution_margin", {
        "groupbyRows": ["first_order_month"],
        "groupbyColumns": ["cohort_number"],
        "metrics": ["avg__contribution_margin_per_customer_usd"],
        "aggregateFunction": "Average",
        "metricsLayout": "COLUMNS",
        "rowOrder": "key_a_to_z",
        "colOrder": "key_a_to_z",
        "row_limit": 10000,
        "time_grain_sqla": "P1M",
        "valueFormat": "$,.2f",
        "date_format": "smart_date",
        "temporal_columns_lookup": {"first_order_month": True},
    }, "Rows: first-order month; columns: 30-day periods since each customer's first paid order; value: cumulative "
       "contribution margin per customer, with each order's refunds, in USD. A period shows once every customer of the month has finished it."),
    chart("New / cancelled / net subscriptions by month", "mixed_timeseries", "exec_daily_brand_metrics", {
        "x_axis": "date_day",
        "time_grain_sqla": "P1M",
        "metrics": ["sum__new_subscriptions", "sum__canceled_subscriptions"],
        "metrics_b": ["sum__net_subscriptions"],
        "groupby": ["brand_name"],
        "groupby_b": ["brand_name"],
        "adhoc_filters_b": [],
        "row_limit": 10000,
        "row_limit_b": 10000,
        "order_desc": True,
        "order_desc_b": True,
        # ECharts series types, not chart plugin names: with a name it does not know, the plugin
        # draws lines (MIXED_SERIES_TYPES)
        "seriesType": "bar",
        "seriesTypeB": "line",
        # The brands' bars on one another: new above the axis, cancelled below it
        "stack": "Stack",
        # One axis for both, so the bars and the lines share their zero
        "yAxisIndex": 0,
        "yAxisIndexB": 0,
        "y_axis_format": "SMART_NUMBER",
        "y_axis_format_secondary": "SMART_NUMBER",
        "show_legend": True,
        "rich_tooltip": True,
        "legendOrientation": "top",
        "legendType": "scroll",
        "x_axis_time_format": "smart_date",
        "color_scheme": "supersetColors",
    }, "Bars: new and cancelled subscriptions; line: net subscriptions. Cancellations are negative."),
    time_series("MRR by month", "exec_mrr_monthly", "echarts_timeseries_line",
                ["sum__closing_mrr_usd"], x_axis="month_start", grain="P1M", fmt="$,.2f",
                description="Monthly recurring revenue: what the subscriptions running at the end of each month are "
                            "worth a month, one unit of each at its price, VAT removed, in USD at constant-currency "
                            "rates. The latest month is the balance on the as-of date."),
    latest_complete_month("Revenue churn, last complete month", "exec_mrr_monthly", "revenue_churn_rate", ".1%",
                          "The MRR of the subscriptions cancelled in the latest month that is over, divided by the "
                          "MRR the month opened with. The line shows the months before it. With several brands "
                          "selected, their MRR is pooled."),
    chart("MRR movements by month", "mixed_timeseries", "exec_mrr_movements", {
        "x_axis": "month_start",
        "time_grain_sqla": "P1M",
        "metrics": ["sum__mrr_usd"],
        "metrics_b": ["net_new_mrr_usd"],
        "groupby": ["movement_type"],
        "groupby_b": [],
        "adhoc_filters_b": [],
        "row_limit": 10000,
        "row_limit_b": 10000,
        "order_desc": True,
        "order_desc_b": True,
        "seriesType": "bar",
        "seriesTypeB": "line",
        "stack": "Stack",
        # One axis for both: the line is the sum of the month's bars
        "yAxisIndex": 0,
        "yAxisIndexB": 0,
        "y_axis_format": "$,.2f",
        "y_axis_format_secondary": "$,.2f",
        "show_legend": True,
        "rich_tooltip": True,
        "legendOrientation": "top",
        "legendType": "scroll",
        "x_axis_time_format": "smart_date",
        "color_scheme": "supersetColors",
    }, "Bars: the MRR that subscriptions starting in the month added (new, or reactivated when a customer came back "
       "after cancelling) and the MRR that cancelled ones took away (churned, below the axis). Line: the net change. "
       "The latest month runs to the as-of date."),
    time_series("Revenue by month at actual and at constant rates", "exec_fx_impact", "echarts_timeseries_line",
                ["sum__revenue_usd_actual", "sum__revenue_usd_constant"], x_axis="month_start", grain="P1M",
                groupby=False, fmt="$,.2f",
                description="Revenue in USD twice: at the constant (budget) rates every other chart uses, and with "
                            "each order and refund at the ECB's rate of its own day. The gap is what the exchange "
                            "rate did. A brand that sells in USD has none. The latest month runs to the as-of date."),
    time_series("Exchange-rate effect by month", "exec_fx_impact", "echarts_timeseries_bar",
                ["sum__fx_effect_usd"], x_axis="month_start", grain="P1M", stack=True, fmt="$,.2f",
                description="Revenue at the rates of the day minus revenue at constant rates, by brand: above "
                            "zero, the brand's currency was worth more dollars than the constant rate says."),
    # "$.3~s" writes $25k: lying down, the last label of the axis is cut at the frame when it is as long as $25,000.00
    time_series("Margin by product", "exec_product_performance", "echarts_timeseries_bar",
                ["sum__line_margin_usd"], x_axis="product_name", grain=None, stack=True, fmt="$.3~s",
                dimension="purchase_type", horizontal=True,
                description="What each product's order lines sold for, VAT removed, less what they cost, over every "
                            "month, by how the product was bought. Before refunds and ad spend. A product with no "
                            "cost in the catalogue shows its whole revenue as margin."),
    time_series("Units by product and month", "exec_product_performance", "echarts_timeseries_bar",
                ["sum__units"], x_axis="month_start", grain="P1M", stack=True, dimension="product_name",
                description="Units on the lines of revenue orders, by the month of the order. The latest month "
                            "runs to the as-of date."),
    time_series("Upsell attach rate by month", "exec_upsell_attach", "echarts_timeseries_line",
                ["upsell_attach_rate"], x_axis="month_start", grain="P1M", groupby=False, fmt=".1%",
                description="Of the customers' first orders placed in the month, the share with an upsell line. "
                            "With several brands selected, their first orders are pooled."),
    time_series("Refund amount by reason and month", "exec_refund_reasons", "echarts_timeseries_bar",
                ["sum__refund_amount_usd"], x_axis="month_start", grain="P1M", stack=True, fmt="$,.2f",
                dimension="refund_reason",
                description="The refunds that come off revenue, VAT removed, in the month they were issued, by "
                            "the reason recorded on the refund."),
    time_series("Average days to refund by reason", "exec_refund_reasons", "echarts_timeseries_bar",
                ["avg_days_to_refund"], x_axis="refund_reason", grain=None, groupby=False, fmt=",.1f", horizontal=True,
                description="Days from the order's date to the refund's, averaged over the refunds of every month."),
    # Every metric of the as-of day, flagged or not: a table of the flagged ones alone would be
    # empty on a normal day, and an empty chart fails the dashboard check.
    chart("Unusual numbers on the as-of day", "table", "exec_kpi_anomalies", {
        "query_mode": "raw",
        "all_columns": ["brand_name", "metric", "value", "baseline_median", "robust_z", "direction",
                        "is_anomaly", "comparison_days"],
        "adhoc_filters": [{
            "clause": "WHERE", "expressionType": "SQL", "sqlExpression": "is_as_of_day = true",
            "subject": None, "operator": None, "comparator": None, "isExtra": False, "isNew": False,
            "filterOptionName": "filter_as_of_day",
        }],
        "order_by_cols": ['["is_anomaly", false]', '["brand_name", true]', '["metric", true]'],
        "row_limit": 1000,
        "server_pagination": False,
        "include_search": False,
        "show_cell_bars": False,
        "allow_render_html": False,
        "table_timestamp_format": "smart_date",
        "column_config": {
            "value": {"d3NumberFormat": ",.2f"},
            "baseline_median": {"d3NumberFormat": ",.2f"},
            "robust_z": {"d3NumberFormat": "+,.2f"},
        },
    }, "Each metric on the as-of date beside its usual value for that weekday (the median of the same weekday in "
       "the eight weeks before). The z-score is the distance in robust standard deviations; it is empty when the "
       "earlier values were all the same. Unusual: a z-score of 3.5 or more and a difference of at least about one "
       "usual day of the metric. The digest leads with the unusual rows."),
]
CHART_BY_NAME = {chart["slice_name"]: chart for chart in CHARTS}


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------
def markdown(name: str, code: str, width: int, height: int = 18) -> dict:
    return {"kind": "markdown", "name": name, "code": code, "width": width, "height": height}


def chart_ref(name: str, width: int, height: int = 50) -> dict:
    return {"kind": "chart", "name": name, "width": width, "height": height}


LAYOUT = [
    [markdown("Introduction", "# Executive Report\n\nRevenue, contribution margin, customers, subscriptions, churn and rebuy for three subscription brands. Every card and chart reads one gold table that already has the shape of the chart; the **Brand** filter on `brand_name` applies to all of them. Amounts are in USD at constant-currency (budget) rates, so brands selling in GBP and USD add up.", 8, 30),
     chart_ref("As-of date", 4, 30)],
    [chart_ref("Orders, latest day", 6, 40), chart_ref("Sales, latest day", 6, 40)],
    [chart_ref("Revenue MTD", 3, 30), chart_ref("Revenue PMTD", 3, 30),
     chart_ref("Revenue YTD", 3, 30), chart_ref("Revenue PYTD", 3, 30)],
    [chart_ref("Revenue by week", 12, 60)],
    [chart_ref("Contribution Margin MTD", 3, 30), chart_ref("Contribution Margin PMTD", 3, 30),
     chart_ref("Contribution Margin YTD", 3, 30), chart_ref("Contribution Margin PYTD", 3, 30)],
    [chart_ref("Contribution margin by week", 12, 60)],
    [chart_ref("Customers MTD", 3, 30), chart_ref("Customers PMTD", 3, 30),
     chart_ref("Customers L30D", 3, 30), chart_ref("Customers L60D", 3, 30)],
    [chart_ref("Customers L90D", 3, 30), chart_ref("New customers by week", 9, 60)],
    [chart_ref("Ad spend by week", 12, 50)],
    [chart_ref("CAC and 90-day LTV by channel", 6, 55), chart_ref("90-day LTV : CAC by channel", 6, 55)],
    [markdown("Storefront heading", "## Storefront\n\nThe shops' websites, from their clickstream: millions of events processed on the lakehouse, where a gate sends each batch to pandas or Spark by its size. Only the daily funnel reaches the warehouse, where it meets the ad spend above.", 12, 24)],
    [chart_ref("Storefront sessions by channel", 7, 55), chart_ref("Storefront funnel by channel", 5, 55)],
    [chart_ref("Net Subscriptions L30D", 3, 30), chart_ref("Net Subscriptions L60D", 3, 30),
     chart_ref("Net Subscriptions L90D", 3, 30), chart_ref("Net Subscriptions L180D", 3, 30)],
    # Side by side: a chart of months with bars keeps its last labels apart at 8 columns or fewer
    [chart_ref("Active subscriptions", 5, 60), chart_ref("New / cancelled / net subscriptions by month", 7, 60)],
    [chart_ref("Churn L30D", 4, 30), chart_ref("Churn L60D", 4, 30),
     chart_ref("Churn L90D", 4, 30)],
    [chart_ref("Cancelled within 30 days", 4, 30), chart_ref("Cancelled within 60 days", 4, 30),
     chart_ref("Cancelled within 90 days", 4, 30)],
    [chart_ref("Subscribers left after N days", 6, 60), chart_ref("Rebuy %", 6, 60)],
    [chart_ref("Contribution margin per customer by cohort", 12, 70)],
    [markdown("Recurring revenue heading", "## Recurring revenue\n\nWhat the subscriptions are worth: monthly recurring revenue (MRR) counts each running subscription at one unit of its product a month, VAT removed. That is what a renewal charges, so a first order's extra units, upsell, shipping and discounts are left out. A month's balance opens where the month before closed, grows by the subscriptions that start and falls by the ones that are cancelled.", 12, 24)],
    [chart_ref("MRR by month", 12, 60)],
    # Not the full width: with 60 px or more per month the axis' last two labels, October and November, touch
    [chart_ref("Revenue churn, last complete month", 4, 60), chart_ref("MRR movements by month", 8, 60)],
    [markdown("Exchange rates heading", "## Exchange rates\n\nEvery other chart in the dashboard converts the brands to USD at one constant (budget) rate per currency, so growth is not affected by day-to-day exchange-rate changes. The two charts below show what the same revenue was worth at the actual rate of each day, including order at the rate of the day it was placed, and refund at the rate of the day it was issued. The forex rates come from ECB's public API, which quotes USD/EUR and GBP/EUR, dividing the first by the second gives USD per GBP.", 12, 28)],
    [chart_ref("Revenue by month at actual and at constant rates", 7, 60), chart_ref("Exchange-rate effect by month", 5, 60)],
    [markdown("Products heading", "## Products\n\nWhich products make the margin, from the lines of revenue orders. A line's margin is its own total without VAT, less its cost. Shipping charged and discounts on a whole order stay with the order, and an order's fixed costs (shipping, processing, payment fees) sit on its first line.", 12, 24)],
    [chart_ref("Margin by product", 12, 60)],
    [chart_ref("Units by product and month", 7, 60), chart_ref("Upsell attach rate by month", 5, 60)],
    [markdown("Refunds heading", "## Refunds\n\nWhy orders are refunded: the refunds that come off revenue, in the month they were issued, by the reason recorded on them.", 12, 20)],
    [chart_ref("Refund amount by reason and month", 8, 60), chart_ref("Average days to refund by reason", 4, 60)],
    [markdown("Unusual numbers heading", "## Unusual numbers\n\nEach brand's revenue, contribution margin, orders, new customers, new and cancelled subscriptions and ad spend on that day, compared against what the brand usually does on that weekday. The rows marked unusual are the ones the daily digest leads with.", 12, 24)],
    [chart_ref("Unusual numbers on the as-of day", 12, 90)],
    [markdown("Footer", "_Rolling and to-date figures are measured from the latest order date (or the dbt `as_of_date` variable). Churn pools the selected brands; the cancelled-within cards and the cohort table average the brands' values._", 12, 18)],
]


def build_position() -> dict:
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"children": ["GRID_ID"], "id": "ROOT_ID", "type": "ROOT"},
        "GRID_ID": {"children": [], "id": "GRID_ID", "parents": ["ROOT_ID"], "type": "GRID", "meta": {}},
    }
    for row_number, row in enumerate(LAYOUT, start=1):
        row_id = f"ROW-{row_number:02d}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "children": [], "id": row_id, "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW", "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for cell_number, cell in enumerate(row, start=1):
            if cell["kind"] == "chart":
                spec = CHART_BY_NAME[cell["name"]]
                node_id = f"CHART-{uuid.uuid5(NAMESPACE, 'node/' + cell['name']).hex[:12]}"
                position[node_id] = {
                    "children": [], "id": node_id,
                    "parents": ["ROOT_ID", "GRID_ID", row_id], "type": "CHART",
                    "meta": {
                        "chartId": 0, "uuid": spec["uuid"], "width": cell["width"],
                        "height": cell["height"], "sliceName": cell["name"],
                    },
                }
            else:
                node_id = f"MARKDOWN-{uuid.uuid5(NAMESPACE, 'markdown/' + cell['name']).hex[:12]}"
                position[node_id] = {
                    "children": [], "id": node_id,
                    "parents": ["ROOT_ID", "GRID_ID", row_id], "type": "MARKDOWN",
                    "meta": {"code": cell["code"], "width": cell["width"], "height": cell["height"]},
                }
            position[row_id]["children"].append(node_id)
    return position


NATIVE_FILTER = {
    "id": "NATIVE_FILTER-brand",
    "name": "Brand",
    "filterType": "filter_select",
    "targets": [
        {"datasetUuid": definition["uuid"], "column": {"name": "brand_name"}}
        for definition in DATASETS.values()
    ],
    "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
    "controlValues": {
        "multiSelect": True,
        "enableEmptyFilter": False,
        "defaultToFirstItem": False,
        "inverseSelection": False,
        "searchAllOptions": False,
    },
    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
    "cascadeParentIds": [],
    "description": "",
    "type": "NATIVE_FILTER",
    "chartsInScope": [],
}


def build_dashboard() -> dict:
    return {
        "dashboard_title": "Executive Report",
        "description": "Revenue, contribution margin, customers, subscriptions, churn and rebuy for the three demo brands.",
        "css": "",
        "slug": SLUG,
        "uuid": DASHBOARD_UUID,
        "position": build_position(),
        "metadata": {
            "native_filter_configuration": [NATIVE_FILTER],
            "chart_configuration": {},
            "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": []},
            "timed_refresh_immune_slices": [],
            "expanded_slices": {},
            "refresh_frequency": 0,
            "default_filters": "{}",
            "color_scheme": "supersetColors",
        },
        "version": VERSION,
        "published": True,
        "certified_by": "",
        "certification_details": "",
    }


def write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=True))


def main() -> None:
    if BUNDLE.exists():
        shutil.rmtree(BUNDLE)
    BUNDLE.mkdir(parents=True)

    write_yaml(BUNDLE / "metadata.yaml", {"version": VERSION, "timestamp": TIMESTAMP})
    write_yaml(BUNDLE / "databases" / "snowflake_local.yaml", {
        "database_name": "Snowflake local (fakesnow)",
        "sqlalchemy_uri": SQLALCHEMY_URI,
        "cache_timeout": None,
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "allow_csv_upload": False,
        "impersonate_user": False,
        "extra": {
            "engine_params": {
                "connect_args": {"host": "127.0.0.1", "port": 64616, "protocol": "http"},
            },
            "allows_virtual_table_explore": True,
        },
        "uuid": DATABASE_UUID,
        "version": VERSION,
    })

    for name, definition in DATASETS.items():
        write_yaml(BUNDLE / "datasets" / f"{name}.yaml", {
            "table_name": name,
            "main_dttm_col": definition["main_dttm_col"],
            "description": definition["description"],
            "default_endpoint": None,
            "offset": 0,
            "cache_timeout": None,
            "schema": "gold",
            "catalog": None,
            "sql": None,
            "params": None,
            "template_params": None,
            "filter_select_enabled": True,
            "fetch_values_predicate": None,
            "extra": None,
            "uuid": definition["uuid"],
            "columns": definition["columns"],
            "metrics": definition["metrics"],
            "version": VERSION,
            "database_uuid": DATABASE_UUID,
            "normalize_columns": False,
            "always_filter_main_dttm": False,
        })

    for spec in CHARTS:
        write_yaml(BUNDLE / "charts" / f"{spec['slice_name'].replace(' ', '_').replace('/', '_')}.yaml", spec)

    write_yaml(BUNDLE / "dashboards" / "executive_report.yaml", build_dashboard())

    # Superset strips the first path component when it reads a bundle, so everything must
    # live under one root directory in the ZIP.
    root = "executive_report"
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(BUNDLE.rglob("*")):
            if path.is_file():
                archive.write(path, f"{root}/{path.relative_to(BUNDLE)}")

    print(f"Wrote {OUT} ({OUT.stat().st_size / 1024:.0f} KB)")
    markdown_blocks = sum(1 for row in LAYOUT for cell in row if cell["kind"] == "markdown")
    print(f"  {len(DATASETS)} datasets, {len(CHARTS)} charts, {markdown_blocks} markdown blocks, 1 native Brand filter")


if __name__ == "__main__":
    main()
