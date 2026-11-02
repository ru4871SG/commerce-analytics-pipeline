"""A stand-in for Snowflake streams and tasks.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from crm_pipeline.bronze_schema import (
    changes_table, identifier, qualified, read_catalog, timestamp_or_none, use_schema,
)
from crm_pipeline.config import BRONZE_DATABASE, BRONZE_SCHEMA

log = logging.getLogger(__name__)

STREAM_OFFSETS = "_stream_offsets"
TASK_HISTORY = "_task_history"
KEY = ("_BRAND", "ID")                    # every incremental table has an id, unique per brand
NOT_COMPARED = {"UPDATED_AT", "_LOADED_AT"}
SAMPLE_ROWS = 5                           # changed rows written to the log per table


def _show(value) -> str:
    """A column value as the change log prints it."""
    if isinstance(value, datetime):
        value = timestamp_or_none(value)
        return "null" if value is None else f"{value:%Y-%m-%d %H:%M:%S}"
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return repr(value) if isinstance(value, str) else str(value)


def stream_name(table: str) -> str:
    return f"{changes_table(table)}_stream".upper()


def task_name(table: str) -> str:
    return f"merge_{table}".upper()


def ensure_metadata(cursor) -> dict[str, list[str]]:
    """Create the offset and history tables when they are missing, and make RAW.CRM the session's
    schema: fakesnow's MERGE creates a helper table in the current schema and fails without one.
    Returns what RAW.CRM holds (bronze_schema.read_catalog), for read_stream: one catalog query
    serves every merge on this connection."""
    catalog = read_catalog(cursor)
    use_schema(cursor, catalog)
    if STREAM_OFFSETS.upper() not in catalog:
        cursor.execute(
            f"create table if not exists {qualified(STREAM_OFFSETS)} "
            "(STREAM_NAME VARCHAR, SOURCE_TABLE VARCHAR, OFFSET_LOADED_AT TIMESTAMP_NTZ, UPDATED_AT TIMESTAMP_NTZ)"
        )
    history = catalog.get(TASK_HISTORY.upper())
    if history is None:
        cursor.execute(
            f"create table if not exists {qualified(TASK_HISTORY)} "
            "(NAME VARCHAR, STATE VARCHAR, ROWS_INSERTED NUMBER, ROWS_UPDATED NUMBER, "
            "OFFSET_FROM TIMESTAMP_NTZ, OFFSET_TO TIMESTAMP_NTZ, COMPLETED_TIME TIMESTAMP_NTZ, "
            "AIRFLOW_RUN_ID VARCHAR, RUN_ID VARCHAR, ORCHESTRATOR VARCHAR, ERROR_MESSAGE VARCHAR)"
        )
    else:
        # Tables created before Dagster existed have AIRFLOW_RUN_ID but not RUN_ID/ORCHESTRATOR.
        for column, data_type in (("RUN_ID", "VARCHAR"), ("ORCHESTRATOR", "VARCHAR")):
            if column not in history:
                cursor.execute(f"alter table {qualified(TASK_HISTORY)} add column {identifier(column)} {data_type}")
    return catalog


def _columns(cursor, table: str) -> list[str]:
    cursor.execute(
        f"select column_name from {BRONZE_DATABASE}.information_schema.columns "
        "where table_schema = %s and table_name = %s order by ordinal_position",
        (BRONZE_SCHEMA, changes_table(table).upper()),
    )
    return [row[0].upper() for row in cursor.fetchall()]


def stream_offset(cursor, table: str) -> datetime | None:
    cursor.execute(
        f"select max(OFFSET_LOADED_AT) from {qualified(STREAM_OFFSETS)} where STREAM_NAME = %s",
        (stream_name(table),),
    )
    return timestamp_or_none(cursor.fetchone()[0])


def _window(offset: datetime | None) -> str:
    return "_LOADED_AT > %s" if offset else "_LOADED_AT is not null"


def _params(offset: datetime | None) -> tuple:
    return (offset,) if offset else ()


def _latest_versions(table: str, columns: list[str], window: str) -> str:
    """The stream's rows, reduced to the latest version of each row: what a MERGE applies."""
    column_list = ", ".join(identifier(c) for c in columns)
    return (
        f"select {column_list} from {qualified(changes_table(table))} where {window} "
        'qualify row_number() over (partition by _BRAND, "ID" order by "UPDATED_AT" desc, _LOADED_AT desc) = 1'
    )


@dataclass
class StreamChanges:
    table: str
    stream: str
    offset: datetime | None
    through: datetime | None = None       # the latest change loaded: where the task consumes up to
    rows: int = 0                         # change rows waiting, versions included
    new: int = 0                          # rows not in RAW.CRM.<TABLE> yet
    changed: int = 0                      # rows that exist and will be updated
    by_brand: dict[str, int] = field(default_factory=dict)
    samples: list[str] = field(default_factory=list)
    columns: list[str] | None = None      # the change table's columns, when the catalog had them

    @property
    def has_data(self) -> bool:
        """SYSTEM$STREAM_HAS_DATA."""
        return self.rows > 0

    def window(self) -> tuple[str, tuple]:
        """The stream's rows, offset (exclusive) to through (inclusive), as SQL and its parameters."""
        return f"{_window(self.offset)} and _LOADED_AT <= %s", _params(self.offset) + (self.through,)


def read_stream(cursor, table: str, catalog: dict[str, list[str]] | None = None,
                describe: bool = True) -> StreamChanges:
    """SELECT ... FROM <TABLE>_CHANGES_STREAM: what changed since the stream was last consumed.
    Reads only; the task consumes the stream. catalog is ensure_metadata()'s answer, which saves
    looking up the change table's columns. With describe=False only the counts are read, which is
    all SYSTEM$STREAM_HAS_DATA needs."""
    offset = stream_offset(cursor, table)
    changes = StreamChanges(table, stream_name(table), offset,
                            columns=(catalog or {}).get(changes_table(table).upper()))
    cursor.execute(
        f"select _BRAND, count(*), max(_LOADED_AT) from {qualified(changes_table(table))} "
        f"where {_window(offset)} group by _BRAND order by _BRAND",
        _params(offset),
    )
    per_brand = cursor.fetchall()
    changes.by_brand = {brand: count for brand, count, _ in per_brand}
    changes.rows = sum(changes.by_brand.values())
    if not changes.has_data:
        return changes
    changes.through = max(loaded for _, _, loaded in per_brand)
    if describe:
        _describe(cursor, changes)
    return changes


def _describe(cursor, changes: StreamChanges) -> None:
    """How many of the stream's rows are new to RAW.CRM.<TABLE> and how many change a row it
    has, and a few of them with the columns that differ: one query, whose window counts are taken
    before the LIMIT."""
    table = changes.table
    if changes.columns is None:
        changes.columns = _columns(cursor, table)
    compared = [c for c in changes.columns if c not in NOT_COMPARED and c not in KEY]
    window, params = changes.window()
    selected = ", ".join(f"s.{identifier(c)}, t.{identifier(c)}" for c in compared)
    cursor.execute(
        f"select s._BRAND, s.\"ID\", t._BRAND is null, {selected}, "
        "count_if(t._BRAND is null) over (), count_if(t._BRAND is not null) over () "
        f"from ({_latest_versions(table, changes.columns, window)}) as s "
        f'left join {qualified(table)} as t on t._BRAND = s._BRAND and t."ID" = s."ID" '
        f'order by t._BRAND is null, s._BRAND, s."ID" limit {SAMPLE_ROWS * 2}',
        params,
    )
    rows = cursor.fetchall()
    changes.new, changes.changed = (int(n) for n in rows[0][-2:])
    for row in rows:
        brand, row_id, is_new, values = row[0], row[1], row[2], row[3:-2]
        if is_new:
            changes.samples.append(f"{brand} id {row_id}: new row")
            continue
        diffs = [
            f"{column.lower()} {_show(old)} -> {_show(new)}"
            for column, new, old in zip(compared, values[0::2], values[1::2])
            if _show(new) != _show(old)
        ]
        changes.samples.append(f"{brand} id {row_id}: " + ("; ".join(diffs) or "updated_at only"))


def log_stream(changes: StreamChanges) -> None:
    if not changes.rows:
        log.info("%s: no rows changed in %s since the stream was last consumed (%s).",
                 changes.stream, changes.table, changes.offset or "never")
        return
    log.info(
        "%s: %s change rows in %s since the stream was last consumed (%s): %s new rows, %s changed rows. By brand: %s",
        changes.stream, changes.rows, changes.table, changes.offset or "never consumed",
        changes.new, changes.changed, ", ".join(f"{b} {n}" for b, n in changes.by_brand.items()),
    )
    for sample in changes.samples:
        log.info("  %s", sample)


@dataclass
class TaskRun:
    name: str
    state: str                            # SUCCEEDED, SKIPPED or FAILED, as in TASK_HISTORY()
    inserted: int = 0
    updated: int = 0


def _record(cursor, run: TaskRun, offset_from, offset_to, run_id: str, orchestrator: str,
            error: str | None = None) -> None:
    cursor.execute(
        f"insert into {qualified(TASK_HISTORY)} "
        "(NAME, STATE, ROWS_INSERTED, ROWS_UPDATED, OFFSET_FROM, OFFSET_TO, COMPLETED_TIME, "
        "AIRFLOW_RUN_ID, RUN_ID, ORCHESTRATOR, ERROR_MESSAGE) "
        "values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (run.name, run.state, run.inserted, run.updated, offset_from, offset_to,
         datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), None, run_id, orchestrator, error),
    )


def _consume(cursor, table: str, through: datetime) -> None:
    """Move the stream's offset to the last change merged, in one transaction."""
    cursor.execute("begin")
    try:
        cursor.execute(f"delete from {qualified(STREAM_OFFSETS)} where STREAM_NAME = %s", (stream_name(table),))
        cursor.execute(
            f"insert into {qualified(STREAM_OFFSETS)} values (%s, %s, %s, %s)",
            (stream_name(table), table, through, datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")),
        )
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise


def run_merge_task(cursor, table: str, run_id: str, orchestrator: str,
                   changes: StreamChanges | None = None) -> TaskRun:
    """One run of the MERGE_<TABLE> task, or SKIPPED when its stream has no data. changes is what
    read_stream() found just before on this cursor; without it the stream's counts are read here."""
    name = task_name(table)
    if changes is None:
        changes = read_stream(cursor, table, describe=False)
    offset = changes.offset
    if not changes.has_data:
        run = TaskRun(name, "SKIPPED")
        _record(cursor, run, offset, offset, run_id, orchestrator)
        log.info("%s skipped: %s has no data.", name, stream_name(table))
        return run

    through = changes.through
    columns = changes.columns or _columns(cursor, table)
    window, params = changes.window()
    updates = ", ".join(f"{identifier(c)} = s.{identifier(c)}" for c in columns if c not in KEY)
    column_list = ", ".join(identifier(c) for c in columns)
    try:
        cursor.execute(
            f"merge into {qualified(table)} as t "
            f"using ({_latest_versions(table, columns, window)}) as s "
            f'on t._BRAND = s._BRAND and t."ID" = s."ID" '
            f"when matched then update set {updates} "
            f"when not matched then insert ({column_list}) values ({', '.join('s.' + identifier(c) for c in columns)})",
            params,
        )
        inserted, updated = (int(n) for n in cursor.fetchone())
        _consume(cursor, table, through)
    except Exception as exc:
        _record(cursor, TaskRun(name, "FAILED"), offset, offset, run_id, orchestrator, str(exc)[:1000])
        raise
    run = TaskRun(name, "SUCCEEDED", inserted, updated)
    _record(cursor, run, offset, through, run_id, orchestrator)
    log.info("%s succeeded: merged %s into %s (%s inserted, %s updated); %s consumed up to %s.",
             name, stream_name(table), table, inserted, updated, stream_name(table), through)
    return run
