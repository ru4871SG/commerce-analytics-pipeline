"""Names in Snowflake's bronze schema, RAW.CRM, and what it holds, read in one query.

The loader (bronze_loader.py) and the stream and task stand-in (stream_task.py) share these. Each
reads the catalog once per connection instead of asking information_schema about every table:
on Snowflake a catalog query takes a noticeable fraction of a second, and a run would otherwise
send dozens.
"""
from __future__ import annotations

from datetime import datetime

from crm_pipeline.config import BRONZE_DATABASE, BRONZE_SCHEMA


def identifier(name: str) -> str:
    # Quoted upper case is what an unquoted name resolves to, so dbt can write `number`
    # and still reach a column whose name is a reserved word.
    return '"' + name.upper().replace('"', '""') + '"'


def qualified(table: str) -> str:
    return f"{BRONZE_DATABASE}.{BRONZE_SCHEMA}.{identifier(table)}"


def changes_table(table: str) -> str:
    """The append-only table an incremental table's changed rows are copied to."""
    return f"{table}_changes"


def timestamp_or_none(value):
    """A timestamp from Snowflake, or None. fakesnow returns a NULL timestamp as 1970-01-01."""
    return None if value is None or value == datetime(1970, 1, 1) else value


def read_catalog(cursor) -> dict[str, list[str]]:
    """Every table in RAW.CRM with its columns in order, upper case. Empty before the first load
    has created the schema."""
    cursor.execute(
        f"select table_name, column_name from {BRONZE_DATABASE}.information_schema.columns "
        "where table_schema = %s order by table_name, ordinal_position",
        (BRONZE_SCHEMA,),
    )
    catalog: dict[str, list[str]] = {}
    for table, column in cursor.fetchall():
        catalog.setdefault(table.upper(), []).append(column.upper())
    return catalog


def use_schema(cursor, catalog: dict[str, list[str]]) -> None:
    """Make RAW.CRM the session's schema, creating it when it holds no table yet. write_pandas
    stages its files in the current schema, and fakesnow's MERGE creates a helper table there."""
    if not catalog:
        cursor.execute(f"create schema if not exists {BRONZE_DATABASE}.{BRONZE_SCHEMA}")
    cursor.execute(f"use schema {BRONZE_DATABASE}.{BRONZE_SCHEMA}")
