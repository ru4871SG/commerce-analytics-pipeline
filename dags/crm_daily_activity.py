"""
Daily shop activity (local demo only)
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import CronTriggerTimetable, dag, get_current_context, task

from crm_pipeline import clickstream, simulator
from crm_pipeline.assets import ACTIVITY
from crm_pipeline.config import SIMULATE_ACTIVITY, TIMEZONE, brands_to_extract
from crm_pipeline.notifications import notify_failure


@task(depends_on_past=True)
def simulate_brand_day(brand_key: str, database: str) -> dict[str, int]:
    """The brand's day in its CRM, then the storefront's clickstream file for it. Both are written
    once: running the day again changes neither."""
    day = simulator.business_day(get_current_context()["logical_date"])
    counts = simulator.simulate_day(brand_key, database, day)
    brand = next(brand for brand in brands_to_extract() if brand.key == brand_key)
    exported = clickstream.export_day(brand, day)
    return {**counts, "clickstream_lines": exported.get("lines", 0)}


@task(outlets=[ACTIVITY])
def activity_recorded(results: list[dict[str, int]]) -> dict[str, int]:
    """Every brand's day is in the CRM. Updating the asset starts the report DAG."""
    total: dict[str, int] = {}
    for result in results:
        for key, value in result.items():
            total[key] = total.get(key, 0) + value
    return total


@dag(
    dag_id="crm_daily_activity",
    schedule=CronTriggerTimetable("0 7 * * *", timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 10, 2, 7, tz=TIMEZONE),
    catchup=True,
    max_active_runs=1,
    default_args={
        "owner": "Ruddy Gunawan",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "on_failure_callback": notify_failure,
    },
    tags=["crm", "demo", "simulator"],
    doc_md=__doc__,
)
def crm_daily_activity():
    days = simulate_brand_day.expand_kwargs(
        [{"brand_key": brand.key, "database": brand.source_database} for brand in brands_to_extract()]
    )
    activity_recorded(days)


if SIMULATE_ACTIVITY:
    crm_daily_activity()
