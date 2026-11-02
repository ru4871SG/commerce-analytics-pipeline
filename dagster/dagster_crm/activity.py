"""The demo's daily shop activity, the Dagster counterpart of crm_daily_activity.
"""
from __future__ import annotations

from datetime import date

from dagster import (
    BackfillPolicy,
    DailyPartitionsDefinition,
    DefaultScheduleStatus,
    MaterializeResult,
    build_schedule_from_partitioned_job,
    define_asset_job,
    asset,
)

from crm_pipeline import clickstream, simulator
from crm_pipeline.config import SIMULATE_ACTIVITY, TIMEZONE, brands_to_extract

from dagster_crm.retries import DEFAULT_RETRY, call

ACTIVITY_PARTITIONS = DailyPartitionsDefinition(start_date="2026-10-01", timezone=TIMEZONE)


@asset(
    key="crm_source_daily_activity",
    group_name="activity",
    partitions_def=ACTIVITY_PARTITIONS,
    backfill_policy=BackfillPolicy.single_run(),
    retry_policy=DEFAULT_RETRY,
    description="Demo only: write each partition day's shop activity into every brand's CRM database, "
                "and its clickstream export into the lakehouse's landing volume.",
)
def crm_source_daily_activity(context) -> MaterializeResult:
    partitions = sorted(context.partition_keys)
    brands = brands_to_extract()
    counts: dict[str, int] = {}
    for partition_key in partitions:
        day = date.fromisoformat(partition_key)
        for brand in brands:
            result = call(simulator.simulate_day, brand.key, brand.source_database, day)
            # The storefront's clickstream file for the day, into the lakehouse's landing volume
            result["clickstream_lines"] = call(clickstream.export_day, brand, day).get("lines", 0)
            for key, value in result.items():
                counts[key] = counts.get(key, 0) + value
    return MaterializeResult(
        metadata={
            "partitions": len(partitions),
            "first_day": partitions[0] if partitions else "",
            "last_day": partitions[-1] if partitions else "",
            "brands": ", ".join(brand.key for brand in brands),
            **counts,
        }
    )


crm_daily_activity_job = define_asset_job(
    "crm_daily_activity",
    selection=[crm_source_daily_activity],
    description="Demo only: write the previous day of shop activity into the CRM databases.",
)

crm_daily_activity_schedule = build_schedule_from_partitioned_job(
    crm_daily_activity_job,
    name="crm_daily_activity_schedule",
    hour_of_day=7,
    minute_of_hour=0,
    default_status=DefaultScheduleStatus.STOPPED,
)


def assets() -> list:
    """The activity asset exists only in demo mode, as crm_daily_activity does in Airflow."""
    return [crm_source_daily_activity] if SIMULATE_ACTIVITY else []


def jobs() -> list:
    return [crm_daily_activity_job] if SIMULATE_ACTIVITY else []


def schedules() -> list:
    return [crm_daily_activity_schedule] if SIMULATE_ACTIVITY else []
