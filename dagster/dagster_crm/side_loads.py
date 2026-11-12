"""The side loads: steps that feed dbt and that the report never waits for.
"""
from __future__ import annotations

from dagster import AssetKey, AssetSpec, MaterializeResult, multi_asset

from crm_pipeline import cdc, fx_rates
from crm_pipeline.config import brands_to_extract

from dagster_crm.resources import SnowflakeWarehouse
from dagster_crm.retries import DEFAULT_RETRY, call

SIDE_LOADS = "crm_side_loads"
FX_RATES = AssetKey(["raw", "ref", "fx_rates"])       # the dbt source ref.fx_rates
ORDERS_CDC = AssetKey(["raw", "crm", "orders_cdc"])   # the dbt source crm.orders_cdc
BRANDS = brands_to_extract()


# The step has the name of Airflow's task; its asset is the table dbt reads as a source
@multi_asset(
    name="load_fx_rates",
    specs=[AssetSpec(FX_RATES, group_name=SIDE_LOADS,
                     description="The ECB's exchange rates for the days after the seed fx_rates_ecb ends: one "
                                 "row per currency and day a rate was published.")],
    retry_policy=DEFAULT_RETRY,
    op_tags={"crm/side_loads": "1"},
    description="The ECB's exchange rates for the days after the latest one there is, appended to "
                "RAW.REF.FX_RATES. Skipped without the ecb_fx_api connection.",
)
def load_fx_rates(context, warehouse: SnowflakeWarehouse) -> MaterializeResult:
    # An answer that cannot be read, or none in three tries, is a PermanentError: no Dagster retry
    result = call(fx_rates.load, warehouse.session)
    context.log.info("FX rates: %s", result)
    return MaterializeResult(asset_key=FX_RATES,
                             metadata={key: value for key, value in result.items() if value is not None})


# One step for every brand, as bronze_merge is one step for every table; Airflow has a task per
# brand. It runs in this job, before or beside the report: what the loader copies after it is
# in the write-ahead log the next capture reads.
@multi_asset(
    name="capture_orders_cdc",
    specs=[AssetSpec(ORDERS_CDC, group_name=SIDE_LOADS,
                     description="Every insert, update and delete of an order since its brand's replication "
                                 "slot exists, as PostgreSQL's write-ahead log has it. Append-only.")],
    retry_policy=DEFAULT_RETRY,
    op_tags={"crm/side_loads": "1"},
    description="The changes of the orders table since each brand's replication slot was last read, decoded "
                "by wal2json and appended to RAW.CRM.ORDERS_CDC. Skipped without the crm_source_cdc connection.",
)
def capture_orders_cdc(context, warehouse: SnowflakeWarehouse) -> MaterializeResult:
    # A slot that is missing or was invalidated is a PermanentError: no Dagster retry. A brand that
    # fails does not stop the others; the step fails once all were tried
    result = call(cdc.capture_all, BRANDS, warehouse.session)
    brands = result.pop("brands")
    context.log.info("CDC orders: %s", result)
    metadata = {key: value for key, value in result.items() if value is not None}
    for brand, captured in brands.items():
        metadata[brand] = (f"{captured['changes']} changes, slot {captured['slot']} at {captured['slot_lsn']}"
                           if "slot" in captured else captured["state"])
    return MaterializeResult(asset_key=ORDERS_CDC, metadata=metadata)


SIDE_LOAD_ASSETS = [load_fx_rates, capture_orders_cdc]
