"""The storefront clickstream on the lakehouse: Airflow's `lakehouse` task group, as one step.
"""
from __future__ import annotations

from dagster import AssetKey, AssetSpec, MaterializeResult, multi_asset

from crm_pipeline import lakehouse

from dagster_crm.resources import SnowflakeWarehouse
from dagster_crm.retries import DEFAULT_RETRY, call

LAKEHOUSE = "lakehouse"
TABLES = {
    AssetKey(["lakehouse", "bronze", "web_events"]): "Storefront clickstream as it landed, one row per line.",
    AssetKey(["lakehouse", "silver", "web_events"]): "One row per valid event, deduplicated.",
    AssetKey(["lakehouse", "gold", "web_daily_funnel"]): "Sessions per brand, day, channel and device, by funnel step.",
    AssetKey(["raw", "web", "daily_funnel"]): "The gold funnel, published into the warehouse for dbt.",
    AssetKey(["raw", "web", "event_batches"]): "One row per clickstream batch: the engine the gate chose, and why.",
}


@multi_asset(
    name="web_events",
    specs=[AssetSpec(key, description=description, group_name=LAKEHOUSE) for key, description in TABLES.items()],
    can_subset=False,
    retry_policy=DEFAULT_RETRY,
    op_tags={"crm/clickstream": "1"},
)
def web_events(context, warehouse: SnowflakeWarehouse):
    forced = context.run.tags.get("crm/web_engine")
    plan = call(lakehouse.plan, forced, "dagster", context.run.run_id)
    result = {}
    if plan["action"] == "run" and plan["engine"] == "pandas":
        result = call(lakehouse.run_in_process, plan, "dagster", context.run.run_id, echo=context.log.info)
    elif plan["action"] == "run":
        result = call(lakehouse.run_on_spark, plan, "dagster", context.run.run_id, echo=context.log.info)
    with warehouse.session() as connection:
        published = call(lakehouse.publish, plan, connection)
    metadata = {
        "action": plan["action"],
        "batch_id": plan["batch_id"] or "",
        "engine": plan["engine"] or "",
        "reason": plan["reason"],
        "estimated_pandas_mib": round(plan["estimated_pandas_bytes"] / 2**20),
        "budget_mib": round(plan["budget_bytes"] / 2**20),
        **{key: result[key] for key in ("bronze_rows", "silver_inserted", "gold_rows") if result.get(key) is not None},
        "published": published.get("published", False),
        "republished": len(published.get("republished", [])),
    }
    for key in TABLES:
        yield MaterializeResult(asset_key=key, metadata=metadata)


LAKEHOUSE_ASSETS = [web_events]
