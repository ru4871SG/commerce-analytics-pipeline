"""CDC of the orders table, from PostgreSQL's write-ahead log, beside the loader.
"""
from __future__ import annotations

import json
import logging
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone

from crm_pipeline import bronze_loader, connections, source_postgres
from crm_pipeline.bronze_schema import identifier
from crm_pipeline.config import SOURCE_CDC_CONN_ID, SOURCE_TABLES, SourceTable, brands_to_extract
from crm_pipeline.errors import PermanentError
from crm_pipeline.raw_tables import ORDERS_CDC_COLUMNS, ORDERS_CDC_META, ORDERS_CDC_TABLE, create_side_tables

log = logging.getLogger(__name__)

PLUGIN = "wal2json"
SOURCE_SCHEMA = "public"

CDC_TABLES: tuple[SourceTable, ...] = tuple(table for table in SOURCE_TABLES if table.name == "orders")
OPERATIONS = {"I": "inserts", "U": "updates", "D": "deletes"}

PEEK_CHANGES = 1000
INSERT_BATCH_ROWS = 500


def slot_name(database: str) -> str:
    """The replication slot of a brand's database. Slot names are unique on the server."""
    return f"cdc_{database}"


def lsn_number(lsn: str) -> int:
    """A place in the write-ahead log as one number. PostgreSQL writes it as two hexadecimal
    halves, 0/404ABE8, which do not sort as text."""
    high, low = lsn.split("/")
    return (int(high, 16) << 32) | int(low, 16)


@dataclass(frozen=True)
class Change:
    """One insert, update or delete of a row, as the write-ahead log has it."""

    op: str                         # I, U or D
    lsn: str                        # the place of the change's own record in the log
    xid: int                        # the transaction that made it
    committed_at: datetime | None   # when that transaction committed, UTC
    values: dict[str, object]       # column to value. A delete has its key only


def _utc(text: str | None) -> datetime | None:
    """wal2json's commit time (2026-11-11 01:01:32.759547+00) as a UTC time without a zone,
    which is what a TIMESTAMP_NTZ column holds."""
    if not text:
        return None
    moment = datetime.fromisoformat(text)
    return moment.astimezone(timezone.utc).replace(tzinfo=None) if moment.tzinfo else moment


def decode(lsn: str, xid: int, data: str, tables: tuple[SourceTable, ...] = CDC_TABLES) -> tuple[str, Change | None]:
    """One row of pg_logical_slot_peek_changes: the action, and the change when it is one.

    Strict, as the exchange rates' parser is: a change of a table that was not asked for, an
    action or a column this module does not know stops the capture before anything of the read
    is written. The error names the table or the column, never a value."""
    message = json.loads(data)
    action = message.get("action")
    if action in ("B", "C"):
        return action, None
    if action not in OPERATIONS:
        raise PermanentError(f"wal2json sent an action this capture does not know at {lsn}: {action!r}.")
    wanted = {table.name: table for table in tables}
    schema, name = message.get("schema"), message.get("table")
    if schema != SOURCE_SCHEMA or name not in wanted:
        # The filter did not apply. What a slot streams without it includes customers.email
        raise PermanentError(
            f"wal2json sent a change of {schema}.{name} at {lsn}, a table that was not asked for "
            f"(add-tables: {add_tables(tables)}). Nothing of this read was written."
        )
    table = wanted[name]
    sent = message["identity"] if action == "D" else message["columns"]
    values = {column["name"].upper(): column["value"] for column in sent if table.keeps(column["name"])}
    unknown = sorted(set(values) - set(ORDERS_CDC_COLUMNS))
    if unknown:
        raise PermanentError(
            f"{schema}.{name} has columns RAW.CRM.ORDERS_CDC does not: {', '.join(unknown)}. Add them to "
            "ORDERS_CDC_COLUMNS in crm_pipeline/raw_tables.py and to the table. Nothing of this read was written."
        )
    if values.get("ID") is None:
        raise PermanentError(f"wal2json sent a change of {schema}.{name} at {lsn} without the row's id.")
    return action, Change(action, lsn, int(xid), _utc(message.get("timestamp")), values)


def add_tables(tables: tuple[SourceTable, ...] = CDC_TABLES) -> str:
    """wal2json's add-tables: only these tables are decoded."""
    return ",".join(f"{SOURCE_SCHEMA}.{table.name}" for table in tables)


def check_slot(cursor, slot: str, database: str) -> str:
    """The slot's confirmed position, after checking that it is there, is wal2json's and still
    has the log it needs. A slot nobody read for too long is invalidated by the server
    (max_slot_wal_keep_size): what it had not read is gone, and no retry brings it back."""
    cursor.execute(
        # to_jsonb: invalidation_reason exists from PostgreSQL 17 on, wal_status from 13
        "select s.plugin, s.database, s.wal_status, to_jsonb(s) ->> 'invalidation_reason', "
        "s.confirmed_flush_lsn::text from pg_replication_slots as s where s.slot_name = %s",
        (slot,),
    )
    row = cursor.fetchone()
    create = f"select pg_create_logical_replication_slot('{slot}', '{PLUGIN}')"
    if row is None:
        raise PermanentError(
            f"The replication slot {slot} does not exist. Create it as a superuser, in {database}: {create}; "
            "(locally: scripts/local.sh source-db up). The capture starts from that moment."
        )
    plugin, slot_database, wal_status, reason, confirmed = row
    if wal_status == "lost" or reason:
        raise PermanentError(
            f"The replication slot {slot} was invalidated ({reason or wal_status}): the server removed the log "
            f"it had not read yet, so the changes of {database} since {confirmed} can no longer be decoded. "
            f"Create it again as a superuser, in {database}: select pg_drop_replication_slot('{slot}'); {create}; "
            "(locally: drop it, then scripts/local.sh source-db up). The capture starts again from that moment: "
            "RAW.CRM.ORDERS_CDC has a gap, the loader's tables do not."
        )
    if plugin != PLUGIN or slot_database != database:
        raise PermanentError(
            f"The replication slot {slot} is a {plugin} slot of {slot_database}, not a {PLUGIN} slot of {database}."
        )
    return confirmed


def peek(cursor, slot: str, tables: tuple[SourceTable, ...] = CDC_TABLES, changes: int = PEEK_CHANGES) -> list[tuple]:
    """The next whole transactions of the slot, as (lsn, xid, message) rows. The slot does not move."""
    cursor.execute(
        "select lsn::text, xid::text, data from pg_logical_slot_peek_changes(%s, NULL, %s, "
        "'format-version', '2', 'add-tables', %s, 'actions', 'insert,update,delete', 'include-timestamp', 'true')",
        (slot, changes, add_tables(tables)),
    )
    return cursor.fetchall()


def read(rows: list[tuple], tables: tuple[SourceTable, ...] = CDC_TABLES) -> tuple[list[Change], str]:
    """The changes of a read, oldest first, and the commit to move the slot to: the lsn of its
    last C row. Rows after that commit, if a read ever ended without one, are left for the
    next read."""
    changes, pending, commit = [], [], None
    for lsn, xid, data in rows:
        action, change = decode(lsn, xid, data, tables)
        if change is not None:
            pending.append(change)
        elif action == "C":
            changes.extend(pending)
            pending, commit = [], lsn
    if commit is None:
        raise RuntimeError(f"A read of {len(rows)} rows from the slot held no commit: the slot was not moved.")
    return changes, commit


def advance(cursor, slot: str, lsn: str) -> None:
    cursor.execute("select end_lsn::text from pg_replication_slot_advance(%s, %s::pg_lsn)", (slot, lsn))
    cursor.fetchone()


def _insert(brand: str, loaded_at: datetime, chunk: list[Change], first_lsn: int) -> tuple[str, list]:
    """One INSERT of a chunk of changes, with the anti-join that drops what the table holds."""
    names = list(ORDERS_CDC_COLUMNS)
    aliases = [f"c{position}" for position in range(len(names))]
    row = "(" + ", ".join(["%s"] * (len(names) + 5)) + ")"
    select = ", ".join(f"v.{alias}::{ORDERS_CDC_COLUMNS[name]}" for alias, name in zip(aliases, names))
    sql = (
        f"insert into {ORDERS_CDC_TABLE} ({', '.join(identifier(name) for name in names)}, "
        f"{', '.join(ORDERS_CDC_META)}) "
        f"select {select}, v.op::varchar, v.lsn::varchar, v.lsn_number::number(38, 0), v.xid::number(38, 0), "
        "v.committed_at::timestamp_ntz, %s, %s::timestamp_ntz "
        f"from (values {', '.join([row] * len(chunk))}) as v ({', '.join(aliases)}, op, lsn, lsn_number, xid, committed_at) "
        f"where not exists (select 1 from {ORDERS_CDC_TABLE} as held where held._BRAND = %s "
        'and held._LSN_NUMBER >= %s and held._LSN_NUMBER = v.lsn_number::number(38, 0) '
        f'and held."ID" = v.{aliases[names.index("ID")]}::number(38, 0))'
    )
    values = [
        value
        for change in chunk
        for value in (
            *(change.values.get(name) for name in names), change.op, change.lsn, lsn_number(change.lsn), change.xid,
            change.committed_at.isoformat(sep=" ") if change.committed_at else None,
        )
    ]
    return sql, [brand, loaded_at.isoformat(sep=" "), *values, brand, first_lsn]


def append(cursor, brand: str, changes: list[Change], loaded_at: datetime) -> int:
    """Append the changes RAW.CRM.ORDERS_CDC does not hold yet, in one transaction: a change is
    one record of the log, so the brand, the LSN and the row's id name it. Returns how many
    were added. Nothing is ever updated or deleted there."""
    if not changes:
        return 0
    first_lsn = min(lsn_number(change.lsn) for change in changes)
    added = 0
    cursor.execute("begin")
    try:
        for start in range(0, len(changes), INSERT_BATCH_ROWS):
            sql, params = _insert(brand, loaded_at, changes[start:start + INSERT_BATCH_ROWS], first_lsn)
            cursor.execute(sql, params)
            added += int(cursor.fetchone()[0])      # Snowflake answers an INSERT with the rows inserted
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise
    return added


def _one(cursor, sql: str, params: tuple = ()):
    cursor.execute(sql, params)
    return cursor.fetchone()[0]


def capture(brand: str, database: str, open_session=bronze_loader.snowflake_session,
            conn_id: str = SOURCE_CDC_CONN_ID, connect=source_postgres.connect,
            tables: tuple[SourceTable, ...] = CDC_TABLES, batch_changes: int = PEEK_CHANGES) -> dict:
    """Read the brand's slot to its end and append the changes of orders to RAW.CRM.ORDERS_CDC.
    Returns what happened: state is SKIPPED (no connection), UP_TO_DATE (nothing new) or
    CAPTURED, with the changes by operation, the LSN range and how far behind the slot is."""
    slot = slot_name(database)
    try:
        source = connect(database, conn_id=conn_id)
    except connections.ConnectionNotFound:
        # Any other error (a server that does not answer, a wrong password) fails the step
        reason = (f"No {conn_id} connection: the write-ahead log is not read. The loader copies the orders "
                  "as before, and RAW.CRM.ORDERS_CDC keeps what it has.")
        log.info("CDC orders: skipped. %s", reason)
        return {"state": "SKIPPED", "reason": reason, "brand": brand, "changes": 0, "added": 0}

    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    counts: Counter = Counter()
    transactions: set[int] = set()
    first_lsn = last_lsn = None
    added = 0
    with source, open_session() as warehouse:
        source.autocommit = True        # each statement on its own: a slot is held only while it is read
        with source.cursor() as slots:
            # wal2json writes values as the session formats them
            slots.execute("set time zone 'UTC'")
            slots.execute("set datestyle = 'ISO, YMD'")
            confirmed = check_slot(slots, slot, database)
            cursor = warehouse.cursor()
            try:
                create_side_tables(cursor)
                while True:
                    flushed = _one(slots, "select pg_current_wal_flush_lsn()::text")
                    rows = peek(slots, slot, tables, batch_changes)
                    if not rows:
                        if lsn_number(flushed) > lsn_number(confirmed):
                            advance(slots, slot, flushed)
                        break
                    changes, commit = read(rows, tables)
                    added += append(cursor, brand, changes, loaded_at)
                    advance(slots, slot, commit)        
                    confirmed = commit
                    for change in changes:
                        counts[change.op] += 1
                        transactions.add(change.xid)
                    if changes:
                        first_lsn, last_lsn = first_lsn or changes[0].lsn, commit
            finally:
                cursor.close()
            slots.execute(
                "select confirmed_flush_lsn::text, (pg_current_wal_flush_lsn() - confirmed_flush_lsn)::bigint "
                "from pg_replication_slots where slot_name = %s", (slot,))
            position, lag = slots.fetchone()

    total = sum(counts.values())
    result = {
        "state": "CAPTURED" if total else "UP_TO_DATE", "brand": brand, "slot": slot, "changes": total,
        **{name: counts[op] for op, name in OPERATIONS.items()}, "transactions": len(transactions), "added": added,
        "first_lsn": first_lsn, "last_lsn": last_lsn, "slot_lsn": position, "lag_bytes": int(lag),
    }
    if total:
        log.info(
            "CDC orders: %s captured %s changes (%s inserts, %s updates, %s deletes) in %s transactions, LSN %s to "
            "%s; %s new in RAW.CRM.ORDERS_CDC. Slot %s is at %s, %s bytes behind the server.",
            brand, total, counts["I"], counts["U"], counts["D"], len(transactions), first_lsn, last_lsn, added,
            slot, position, lag,
        )
    else:
        log.info("CDC orders: %s has nothing new. Slot %s is at %s, %s bytes behind the server.",
                 brand, slot, position, lag)
    return result


def capture_all(brands=None, open_session=bronze_loader.snowflake_session, capture_brand=capture) -> dict:
    """Every extracted brand's capture, one after the other: what a step that covers all brands
    runs (Dagster's; Airflow has a task per brand). A brand that fails does not stop the ones
    after it; the first failure is raised once all were tried. Returns the totals, and each
    brand's result under its key."""
    brands = brands_to_extract() if brands is None else brands
    results, failures = {}, []
    for brand in brands:
        try:
            results[brand.key] = capture_brand(brand.key, brand.source_database, open_session)
        except Exception as error:      # raised below, once the other brands were tried
            log.error("CDC orders: %s failed: %s", brand.key, error)
            failures.append(error)
    if failures:
        # A slot that is gone stays gone, a permanent failure wins over one a retry could cure
        raise next((error for error in failures if isinstance(error, PermanentError)), failures[0])
    states = {result["state"] for result in results.values()}
    state = "CAPTURED" if "CAPTURED" in states else "SKIPPED" if states == {"SKIPPED"} else "UP_TO_DATE"
    totals = {name: sum(result.get(name, 0) for result in results.values())
              for name in ("changes", *OPERATIONS.values(), "added")}
    return {"state": state, **totals,
            "lag_bytes": max((result.get("lag_bytes", 0) for result in results.values()), default=0),
            "brands": results}


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if argv:
        print("usage: python -m crm_pipeline.cdc", file=sys.stderr)
        return 2
    capture_all()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
