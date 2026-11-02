"""The Delta tables, read and written without Spark: delta-rs (the `deltalake` package) and pyarrow.
"""
from __future__ import annotations

from datetime import date

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, QueryBuilder, write_deltalake
from deltalake.exceptions import TableNotFoundError

from crm_lakehouse.contract import Table, table_properties
from crm_lakehouse.layout import Lakehouse

ARROW_TYPES = {
    "string": pa.string(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "timestamp": pa.timestamp("us", tz="UTC"),
    "date": pa.date32(),
}

def arrow_schema(table: Table) -> pa.Schema:
    return pa.schema([pa.field(column.name, ARROW_TYPES[column.type]) for column in table.columns])


def to_arrow(frame: pd.DataFrame, table: Table) -> pa.Table:
    """A frame with the table's columns as an Arrow table of exactly the table's schema."""
    schema = arrow_schema(table)
    arrays = []
    for column in schema:
        values = frame[column.name] if column.name in frame else pd.Series([None] * len(frame), dtype="object")
        if pa.types.is_timestamp(column.type):
            values = pd.to_datetime(values, utc=True)
        elif pa.types.is_date32(column.type):
            values = pd.Series([v if v is None or pd.isna(v) else pd.Timestamp(v).date() for v in values],
                               dtype="object")
        arrays.append(pa.array(values, type=column.type, from_pandas=True))
    return pa.Table.from_arrays(arrays, schema=schema)


def open_table(lakehouse: Lakehouse, table: Table, operation: str = "READ") -> DeltaTable | None:
    try:
        return DeltaTable(lakehouse.uri(table), storage_options=lakehouse.storage_options(table, operation))
    except TableNotFoundError:
        return None


def ensure_table(lakehouse: Lakehouse, table: Table) -> DeltaTable:
    """The table, created empty with the contract's schema and properties when it is missing."""
    existing = open_table(lakehouse, table, "READ_WRITE")
    if existing is not None:
        return existing
    return DeltaTable.create(
        lakehouse.uri(table),
        schema=arrow_schema(table),
        mode="ignore",                       # another writer may have created it meanwhile
        name=table.name,
        description=table.comment,
        configuration=table_properties(table),
        storage_options=lakehouse.storage_options(table, "READ_WRITE"),
    )


def literal(value) -> str:
    """A value as a SQL literal, for the predicates below."""
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    return "'" + str(value).replace("'", "''") + "'"


def query(lakehouse: Lakehouse, table: Table, sql: str) -> pa.Table:
    """Run sql over the table, registered as t, with delta-rs's query engine (DataFusion), which
    skips the files whose statistics rule out the WHERE clause. None when the table does not exist
    yet. Text comes back as plain strings (delta-rs reads it as string_view, which pyarrow's
    compute functions do not all take)."""
    delta = open_table(lakehouse, table)
    if delta is None:
        return None
    data = pa.table(QueryBuilder().register("t", delta).execute(sql))
    fields = [column.with_type(pa.string()) if pa.types.is_string_view(column.type)
              or pa.types.is_large_string(column.type) else column for column in data.schema]
    return data.cast(pa.schema(fields))


def read(lakehouse: Lakehouse, table: Table, where: str | None = None, columns: list[str] | None = None) -> pa.Table:
    """The table's rows that match where (SQL), with the contract's column types; an empty table
    of that schema when the table does not exist yet."""
    schema = arrow_schema(table)
    if columns:
        schema = pa.schema([schema.field(name) for name in columns])
    data = query(lakehouse, table, f"SELECT {', '.join(schema.names)} FROM t" + (f" WHERE {where}" if where else ""))
    return schema.empty_table() if data is None else data.cast(schema)


def distinct_values(lakehouse: Lakehouse, table: Table, column: str, where: str | None = None) -> set:
    data = query(lakehouse, table, f"SELECT DISTINCT {column} FROM t" + (f" WHERE {where}" if where else ""))
    return set() if data is None else set(data.column(column).to_pylist()) - {None}


def append(lakehouse: Lakehouse, table: Table, data: pa.Table) -> None:
    ensure_table(lakehouse, table)
    write_deltalake(lakehouse.uri(table), data, mode="append",
                    storage_options=lakehouse.storage_options(table, "READ_WRITE"))


def merge_insert_new(lakehouse: Lakehouse, table: Table, data: pa.Table, keys: tuple[str, ...]) -> int:
    """Insert the rows whose keys the table does not hold yet; returns how many were inserted."""
    delta = ensure_table(lakehouse, table)
    if not data.num_rows:
        return 0
    predicate = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    metrics = (
        delta.merge(source=data, predicate=predicate, source_alias="s", target_alias="t")
        .when_not_matched_insert_all()
        .execute()
    )
    return int(metrics.get("num_target_rows_inserted", 0))


def replace_days(lakehouse: Lakehouse, table: Table, data: pa.Table, first: date, last: date) -> None:
    """Replace the rows of business days first..last with data, in one commit (Spark's
    replaceWhere). Days in the range that data does not cover end up with no rows."""
    ensure_table(lakehouse, table)
    write_deltalake(
        lakehouse.uri(table), data, mode="overwrite",
        predicate=f"business_date >= '{first.isoformat()}' AND business_date <= '{last.isoformat()}'",
        storage_options=lakehouse.storage_options(table, "READ_WRITE"),
    )
