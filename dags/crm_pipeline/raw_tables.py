"""The raw tables that steps dbt does not wait for write, created before dbt needs them.
"""
from __future__ import annotations

import logging

from crm_lakehouse.contract import GOLD_FUNNEL
from crm_pipeline import bronze_loader
from crm_pipeline.bronze_schema import identifier
from crm_pipeline.config import BRONZE_DATABASE, BRONZE_SCHEMA, REF_SCHEMA, WEB_SCHEMA

log = logging.getLogger(__name__)

FX_RATES_TABLE = f"{BRONZE_DATABASE}.{REF_SCHEMA}.FX_RATES"
ORDERS_CDC_TABLE = f"{BRONZE_DATABASE}.{BRONZE_SCHEMA}.ORDERS_CDC"
WEB_FUNNEL_TABLE = f"{BRONZE_DATABASE}.{WEB_SCHEMA}.{identifier('daily_funnel')}"
WEB_BATCHES_TABLE = f"{BRONZE_DATABASE}.{WEB_SCHEMA}.{identifier('event_batches')}"

# The columns of the source's orders table, with the types the loader gives them in
# RAW.CRM.ORDERS (bronze_loader.snowflake_type). A fixed list, not read from the source as the
# loader's is: the dbt step creates this table, and it has no connection to the source. A column
# the source gains is added here; until then the capture stops and names it (cdc.decode), and
# tests/test_cdc.py holds this list equal to the orders table of fake_crm.sql.
ORDERS_CDC_COLUMNS: dict[str, str] = {
    "ID": "NUMBER(38, 0)",
    "NUMBER": "VARCHAR",
    "CUSTOMER_ID": "NUMBER(38, 0)",
    "CREATED_FROM_ORDER_ID": "NUMBER(38, 0)",
    "STATE": "VARCHAR",
    "PAYMENT_STATE": "VARCHAR",
    "SHIPPING_STATE": "VARCHAR",
    "CHECKOUT_COMPLETED_AT": "TIMESTAMP_NTZ",
    "ITEMS_TOTAL": "NUMBER(38, 0)",
    "ADJUSTMENTS_TOTAL": "NUMBER(38, 0)",
    "TOTAL": "NUMBER(38, 0)",
    "CURRENCY_CODE": "VARCHAR",
    "IS_SUBSCRIPTION": "BOOLEAN",
    "IS_SUBSCRIPTION_CANCELED": "BOOLEAN",
    "SUBSCRIPTION_CANCELED_AT": "TIMESTAMP_NTZ",
    "SUBSCRIPTION_PAUSED": "BOOLEAN",
    "IS_CREATED_BY_ADMIN": "BOOLEAN",
    "IS_SALES_FUNNEL": "BOOLEAN",
    "ACQUISITION_DOMAIN": "VARCHAR",
    "UTM_SOURCE": "VARCHAR",
    "UTM_MEDIUM": "VARCHAR",
    "UTM_CAMPAIGN": "VARCHAR",
    "UTM_CONTENT": "VARCHAR",
    "UTM_TERM": "VARCHAR",
    "UTM_ID": "VARCHAR",
    "CREATED_AT": "TIMESTAMP_NTZ",
    "UPDATED_AT": "TIMESTAMP_NTZ",
}
# What the capture adds to every change
ORDERS_CDC_META: dict[str, str] = {
    "_OP": "VARCHAR",                   # I, U or D: insert, update, delete
    "_LSN": "VARCHAR",                  # the change's place in the write-ahead log, as PostgreSQL writes it (0/404ABE8)
    "_LSN_NUMBER": "NUMBER(38, 0)",     # the same as a number: the order the changes happened in
    "_XID": "NUMBER(38, 0)",            # the transaction that made it
    "_COMMITTED_AT": "TIMESTAMP_NTZ",   # when that transaction committed, UTC
    "_BRAND": "VARCHAR",
    "_LOADED_AT": "TIMESTAMP_NTZ",      # when the capture that read it started, UTC
}

# The clickstream's two tables. RAW.WEB.DAILY_FUNNEL has the columns of the lakehouse's gold
# (crm_lakehouse.contract), in the warehouse's types; RAW.WEB.EVENT_BATCHES one row per batch,
# from the lakehouse's batch ledger: what the planner decided and what the engine did.
LAKEHOUSE_TYPES = {"string": "VARCHAR", "bigint": "NUMBER(38, 0)", "double": "FLOAT", "date": "DATE",
                   "timestamp": "TIMESTAMP_NTZ"}
WEB_BATCH_COLUMNS = ("batch_id", "engine", "reason", "files", "compressed_bytes", "estimated_rows",
                     "estimated_pandas_bytes", "budget_bytes", "bronze_rows", "silver_inserted", "dropped_json",
                     "gold_from", "gold_to", "gold_rows", "observed_pandas_bytes", "seconds", "contract",
                     "orchestrator", "run_id", "planned_at", "succeeded_at")
_WEB_BATCH_TYPES = {
    **{name: "NUMBER(38, 0)" for name in WEB_BATCH_COLUMNS}, "seconds": "FLOAT", "gold_from": "DATE",
    "gold_to": "DATE", "planned_at": "TIMESTAMP_NTZ", "succeeded_at": "TIMESTAMP_NTZ",
    **{name: "VARCHAR" for name in ("batch_id", "engine", "reason", "dropped_json", "contract", "orchestrator",
                                    "run_id")},
}

# schema -> table -> its columns. Each side load adds its own here.
SIDE_TABLES: dict[str, dict[str, str]] = {
    f"{BRONZE_DATABASE}.{REF_SCHEMA}": {
        # One row per currency and day the ECB published a rate: units of the currency per euro
        FX_RATES_TABLE: "RATE_DATE DATE, CURRENCY_CODE VARCHAR, EUR_RATE NUMBER(18, 6), _LOADED_AT TIMESTAMP_NTZ",
    },
    f"{BRONZE_DATABASE}.{BRONZE_SCHEMA}": {
        # One row per insert, update and delete of an order, as the write-ahead log has it. The
        # names are quoted, as the loader's are: NUMBER is a column of orders and a type
        ORDERS_CDC_TABLE: ", ".join(
            [f'"{name}" {kind}' for name, kind in ORDERS_CDC_COLUMNS.items()]
            + [f"{name} {kind}" for name, kind in ORDERS_CDC_META.items()]),
    },
    f"{BRONZE_DATABASE}.{WEB_SCHEMA}": {
        WEB_FUNNEL_TABLE: ", ".join(
            [f"{identifier(column.name)} {LAKEHOUSE_TYPES[column.type]}" for column in GOLD_FUNNEL.columns]
            + ["_LOADED_AT TIMESTAMP_NTZ"]),
        WEB_BATCHES_TABLE: ", ".join(
            [f"{identifier(name)} {_WEB_BATCH_TYPES[name]}" for name in WEB_BATCH_COLUMNS]
            + ["_LOADED_AT TIMESTAMP_NTZ"]),
    },
}


def create_side_tables(cursor) -> list[str]:
    """Every schema and table of SIDE_TABLES, when it is missing. Returns the tables' names."""
    tables = []
    for schema, schema_tables in SIDE_TABLES.items():
        cursor.execute(f"create schema if not exists {schema}")
        for table, columns in schema_tables.items():
            cursor.execute(f"create table if not exists {table} ({columns})")
            tables.append(table)
    return tables


def ensure_side_tables(session=bronze_loader.snowflake_session) -> list[str]:
    """create_side_tables() in a session of its own: what the dbt step of each orchestrator calls."""
    with session() as connection:
        cursor = connection.cursor()
        try:
            tables = create_side_tables(cursor)
        finally:
            cursor.close()
    log.info("The tables dbt does not wait for are there: %s", ", ".join(tables))
    return tables
