"""Dagster definitions: the activity asset, bronze, the lakehouse, dbt, activation, and their automation."""
from __future__ import annotations

from dagster import Definitions

from dagster_crm import activity, activation, automation, bronze, dbt_assets, lakehouse, side_loads
from dagster_crm.resources import SnowflakeWarehouse

defs = Definitions(
    assets=[
        *activity.assets(),
        *bronze.BRONZE_ASSETS,
        *lakehouse.LAKEHOUSE_ASSETS,
        *side_loads.SIDE_LOAD_ASSETS,
        dbt_assets.crm_dbt_assets,
        *activation.ACTIVATION_ASSETS,
    ],
    jobs=[*activity.jobs(), *automation.jobs()],
    schedules=[*activity.schedules(), *automation.schedules()],
    sensors=[*automation.sensors()],
    resources={
        "warehouse": SnowflakeWarehouse(),
        "dbt": dbt_assets.dbt_resource,
    },
)
