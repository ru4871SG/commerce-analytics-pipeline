"""The storefront clickstream
"""
from __future__ import annotations

import json
import logging
from collections.abc import Callable
from datetime import date, datetime, timezone

from crm_lakehouse import delta_io, pandas_engine, planning, runner, unity_catalog
from crm_lakehouse.contract import GOLD_FUNNEL, PUBLISHED, SUCCEEDED
from crm_lakehouse.layout import Lakehouse
from crm_pipeline import connections
from crm_pipeline.bronze_schema import identifier
from crm_pipeline.config import (
    BRONZE_DATABASE, DATABRICKS_CONN_ID, LAKEHOUSE_CATALOG, LAKEHOUSE_ROOT, LOCAL_WORKSPACE, PANDAS_BUDGET_BYTES,
    SPARK_PYTHON, WEB_SCHEMA,
)
from crm_pipeline.errors import PermanentError

log = logging.getLogger(__name__)

DEFAULT_JOB_NAME = "web_events_spark"
INSERT_BATCH_ROWS = 500
HELD_CHUNK = 1_000          # batch ids per lookup in RAW.WEB.EVENT_BATCHES

FUNNEL_TABLE = "daily_funnel"
BATCHES_TABLE = "event_batches"
FUNNEL_COLUMNS = [column.name for column in GOLD_FUNNEL.columns]
BATCH_COLUMNS = ("batch_id", "engine", "reason", "files", "compressed_bytes", "estimated_rows",
                 "estimated_pandas_bytes", "budget_bytes", "bronze_rows", "silver_inserted", "dropped_json",
                 "gold_from", "gold_to", "gold_rows", "observed_pandas_bytes", "seconds", "contract",
                 "orchestrator", "run_id", "planned_at", "succeeded_at")
SNOWFLAKE_TYPES = {"string": "VARCHAR", "bigint": "NUMBER(38, 0)", "double": "FLOAT", "date": "DATE",
                   "timestamp": "TIMESTAMP_NTZ"}


# --- where the lakehouse is ----------------------------------------------------------------

def workspace():
    """A Databricks workspace client signed in as the orchestrator's service principal, or None
    when no databricks_default connection is configured (the local demo)."""
    try:
        conn = connections.get_connection(DATABRICKS_CONN_ID)
    except connections.ConnectionNotFound:
        return None
    from databricks.sdk import WorkspaceClient

    host = conn.host or conn.extra.get("host", "")
    settings = {"host": host if host.startswith("http") else f"https://{host}"}
    if conn.login:
        settings["client_id"] = conn.login
    if conn.password:
        settings["client_secret"] = conn.password
    if conn.extra.get("auth_type"):
        settings["auth_type"] = conn.extra["auth_type"]
    return WorkspaceClient(**settings)


def open_lakehouse() -> tuple[Lakehouse, object | None]:
    client = workspace()
    if client is None:
        return Lakehouse(LAKEHOUSE_CATALOG, root=LAKEHOUSE_ROOT), None
    extra = connections.get_connection(DATABRICKS_CONN_ID).extra
    for key in ("catalog", "storage_root"):
        if not extra.get(key):
            raise PermanentError(f"The {DATABRICKS_CONN_ID} connection's extra has no {key}.")
    lakehouse = Lakehouse(
        extra["catalog"], storage_root=extra["storage_root"],
        credentials=unity_catalog.vended_credentials(client, extra.get("region")),
        files=unity_catalog.VolumeFiles(client),
    )
    return lakehouse, client


def _plain(row: dict) -> dict:
    """A ledger row as XCom and Dagster metadata can hold it."""
    return {key: value.isoformat() if isinstance(value, (date, datetime)) else value for key, value in row.items()
            if key != "files_json"}


# --- plan, run, publish --------------------------------------------------------------------

def plan(forced: str | None, orchestrator: str, run_id: str) -> dict:
    """The next batch and its engine, recorded in the ledger (crm_lakehouse.planning)."""
    lakehouse, _ = open_lakehouse()
    planned = planning.plan_batch(lakehouse, PANDAS_BUDGET_BYTES, forced or None, orchestrator, run_id)
    log.info("Clickstream: %s%s%s. %s", planned.action, f" batch {planned.batch_id}" if planned.batch_id else "",
             f" on {planned.engine}" if planned.engine else "", planned.reason)
    return planned.to_dict()


def next_step(plan_values: dict) -> str:
    """The step after the plan: run_in_process, run_on_spark or publish_web_funnel."""
    if plan_values["action"] != "run":
        return "publish_web_funnel"
    return "run_in_process" if plan_values["engine"] == planning.PANDAS else "run_on_spark"


def run_in_process(plan_values: dict, orchestrator: str, run_id: str, try_number: int = 1,
                   echo: Callable[[str], None] = log.info) -> dict:
    """The batch on the in-process engine. A retry does not try in process again: the attempt
    that failed may have run out of memory, so the planner moves the batch to Spark."""
    if try_number > 1:
        resumed = plan(None, orchestrator, run_id)
        echo(f"Try {try_number}: {resumed['reason']}")
        if resumed["action"] != "run":
            return _latest(resumed["batch_id"] or plan_values["batch_id"])
        if resumed["engine"] == planning.SPARK:
            return run_on_spark(resumed, orchestrator, run_id, echo)
        plan_values = resumed
    lakehouse, _ = open_lakehouse()
    return _plain(pandas_engine.run_batch(lakehouse, plan_values["batch_id"], orchestrator, run_id))


def run_on_spark(plan_values: dict, orchestrator: str, run_id: str, echo: Callable[[str], None] = log.info) -> dict:
    """The batch on Spark: the bundle's Databricks job, or Spark in .venv-spark locally."""
    lakehouse, client = open_lakehouse()
    if client is None and LOCAL_WORKSPACE:
        # The bundle's job, deployed to the local workspace: the released wheel, not the source tree
        from databricks.sdk import WorkspaceClient

        local = WorkspaceClient(host=LOCAL_WORKSPACE, token="local-workspace", auth_type="pat")
        runner.run_on_databricks(local, DEFAULT_JOB_NAME, plan_values["batch_id"], echo=echo)
    elif client is None:
        runner.run_local(lakehouse, plan_values["batch_id"], SPARK_PYTHON, orchestrator, run_id, echo)
    else:
        job_name = connections.get_connection(DATABRICKS_CONN_ID).extra.get("job_name", DEFAULT_JOB_NAME)
        runner.run_on_databricks(client, job_name, plan_values["batch_id"], echo=echo)
    return _latest(plan_values["batch_id"])


def _latest(batch_id: str) -> dict:
    lakehouse, _ = open_lakehouse()
    latest = planning.latest_states(planning.ledger(lakehouse))[batch_id]
    if latest["state"] not in (SUCCEEDED, PUBLISHED):
        raise RuntimeError(f"Batch {batch_id} ended {latest['state']}: {latest['error']}")
    return _plain(latest)


def _qualified(table: str) -> str:
    return f"{BRONZE_DATABASE}.{WEB_SCHEMA}.{identifier(table)}"


def ensure_web_tables(cursor) -> None:
    """RAW.WEB and its two tables, when they are missing."""
    cursor.execute(f"create schema if not exists {BRONZE_DATABASE}.{WEB_SCHEMA}")
    funnel = ", ".join(f"{identifier(c.name)} {SNOWFLAKE_TYPES[c.type]}" for c in GOLD_FUNNEL.columns)
    cursor.execute(f"create table if not exists {_qualified(FUNNEL_TABLE)} ({funnel}, _LOADED_AT TIMESTAMP_NTZ)")
    types = {**{name: "NUMBER(38, 0)" for name in BATCH_COLUMNS}, "seconds": "FLOAT", "gold_from": "DATE",
             "gold_to": "DATE", "planned_at": "TIMESTAMP_NTZ", "succeeded_at": "TIMESTAMP_NTZ",
             **{name: "VARCHAR" for name in ("batch_id", "engine", "reason", "dropped_json", "contract",
                                             "orchestrator", "run_id")}}
    batches = ", ".join(f"{identifier(name)} {types[name]}" for name in BATCH_COLUMNS)
    cursor.execute(f"create table if not exists {_qualified(BATCHES_TABLE)} ({batches}, _LOADED_AT TIMESTAMP_NTZ)")


def _insert(cursor, table: str, columns, rows: list[tuple]) -> None:
    placeholders = ", ".join(["%s"] * (len(columns) + 1))
    names = ", ".join(identifier(name) for name in columns) + ", _LOADED_AT"
    for start in range(0, len(rows), INSERT_BATCH_ROWS):
        cursor.executemany(f"insert into {_qualified(table)} ({names}) values ({placeholders})",
                           rows[start:start + INSERT_BATCH_ROWS])


def _naive_utc(value):
    return value.astimezone(timezone.utc).replace(tzinfo=None) if isinstance(value, datetime) else value


def _copy_batch(lakehouse: Lakehouse, rows: list[dict], batch_id: str, cursor) -> list[dict]:
    """Replace the batch's gold days in RAW.WEB.DAILY_FUNNEL with the lakehouse's gold for those
    days, and its summary in RAW.WEB.EVENT_BATCHES, in one transaction. Returns the gold rows."""
    succeeded = [row for row in rows if row["batch_id"] == batch_id and row["state"] == SUCCEEDED][-1]
    first, last = succeeded["gold_from"], succeeded["gold_to"]
    gold = []
    if first is not None:
        gold = delta_io.read(lakehouse, GOLD_FUNNEL,
                             f"business_date BETWEEN {delta_io.literal(first)} AND {delta_io.literal(last)}").to_pylist()
    planned = [row for row in rows if row["batch_id"] == batch_id and row["state"] == "PLANNED"][-1]
    summary = {**planned, **{k: v for k, v in succeeded.items() if v is not None},
               "planned_at": planned["recorded_at"], "succeeded_at": succeeded["recorded_at"]}
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    cursor.execute("begin")
    try:
        if first is not None:
            cursor.execute(f"delete from {_qualified(FUNNEL_TABLE)} where BUSINESS_DATE between %s and %s",
                           (first, last))
            _insert(cursor, FUNNEL_TABLE, FUNNEL_COLUMNS,
                    [tuple(row[name] for name in FUNNEL_COLUMNS) + (loaded_at,) for row in gold])
        cursor.execute(f"delete from {_qualified(BATCHES_TABLE)} where BATCH_ID = %s", (batch_id,))
        _insert(cursor, BATCHES_TABLE, BATCH_COLUMNS,
                [tuple(_naive_utc(summary.get(name)) for name in BATCH_COLUMNS) + (loaded_at,)])
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise
    return gold


def _held_batches(cursor, batch_ids: list[str]) -> set[str]:
    """Which of these batches RAW.WEB.EVENT_BATCHES holds. Asked in chunks, so that no answer
    reaches the emulator's cap of 2,048 rows."""
    held = set()
    for start in range(0, len(batch_ids), HELD_CHUNK):
        chunk = batch_ids[start:start + HELD_CHUNK]
        cursor.execute(f"select BATCH_ID from {_qualified(BATCHES_TABLE)} "
                       f"where BATCH_ID in ({', '.join(['%s'] * len(chunk))})", chunk)
        held.update(row[0] for row in cursor.fetchall())
    return held


def republish_missing(lakehouse: Lakehouse, rows: list[dict], cursor) -> list[str]:
    """Copy again, oldest first, every batch the ledger records as published but RAW.WEB does not
    hold: after a warehouse reset (scripts/local.sh warehouse reset, which demo runs after a source
    rebuild), or a RAW.WEB lost on Snowflake. The planner reads only the lakehouse, so it would
    never send those batches again. Each copy takes the current gold of the batch's days. Returns
    the batch ids copied."""
    published = [batch_id for batch_id, latest in planning.latest_states(rows).items() if latest["state"] == PUBLISHED]
    held = _held_batches(cursor, published)
    missing = [batch_id for batch_id in published if batch_id not in held]
    for batch_id in missing:
        gold = _copy_batch(lakehouse, rows, batch_id, cursor)
        log.info("Clickstream: RAW.WEB lacked published batch %s; copied it again: %s gold rows.", batch_id, len(gold))
    return missing


def publish(plan_values: dict, snowflake_conn) -> dict:
    """Copy the batch's gold days into RAW.WEB.DAILY_FUNNEL and its ledger summary into
    RAW.WEB.EVENT_BATCHES, replacing what they held for those days and that batch, in one
    transaction; then record the batch as PUBLISHED. First, every published batch RAW.WEB lacks is
    copied again (republish_missing). Nothing else to do when nothing was planned."""
    batch_id = plan_values.get("batch_id")
    lakehouse, _ = open_lakehouse()
    rows = planning.ledger(lakehouse)
    cursor = snowflake_conn.cursor()
    try:
        # The tables exist from the first run on, so dbt's sources do even before any clickstream
        ensure_web_tables(cursor)
        republished = republish_missing(lakehouse, rows, cursor)
        if plan_values["action"] == "nothing" or not batch_id:
            log.info("Clickstream: nothing to publish (%s).", plan_values["reason"])
            return {"published": False, "reason": plan_values["reason"], "republished": republished}
        latest = planning.latest_states(rows)[batch_id]
        if latest["state"] == PUBLISHED:
            return {"published": False, "reason": f"{batch_id} was already published", "republished": republished}
        if latest["state"] != SUCCEEDED:
            raise PermanentError(f"Batch {batch_id} is {latest['state']}, not SUCCEEDED: nothing to publish.")
        gold = _copy_batch(lakehouse, rows, batch_id, cursor)
    finally:
        cursor.close()
    first, last = latest["gold_from"], latest["gold_to"]
    planning.record(lakehouse, batch_id, PUBLISHED, engine=latest["engine"], gold_from=first, gold_to=last,
                    gold_rows=len(gold))
    log.info("Clickstream batch %s (%s) published: %s gold rows for %s..%s", batch_id, latest["engine"],
             len(gold), first, last)
    return {"published": True, "batch_id": batch_id, "engine": latest["engine"], "gold_rows": len(gold),
            "gold_from": str(first) if first else None, "gold_to": str(last) if last else None,
            "dropped": json.loads(latest["dropped_json"] or "{}"), "republished": republished}


# --- scripts/local.sh lakehouse run | status -----------------------------------------------

def status(limit: int = 8) -> list[str]:
    """The lakehouse at a glance: waiting files, table sizes, the latest batches."""
    lakehouse, _ = open_lakehouse()
    from crm_lakehouse.contract import BRONZE_EVENTS, SILVER_EVENTS

    counts = {}
    for table in (BRONZE_EVENTS, SILVER_EVENTS, GOLD_FUNNEL):
        data = delta_io.query(lakehouse, table, "SELECT count(*) AS n FROM t")
        counts[lakehouse.name(table)] = 0 if data is None else data.column("n")[0].as_py()
    pending = planning.pending_files(lakehouse)
    lines = [f"{len(pending)} landing files waiting ({sum(f.size for f in pending) / 2**20:,.1f} MiB); "
             + ", ".join(f"{name} {rows:,}" for name, rows in counts.items())]
    rows = planning.ledger(lakehouse)
    batches = list(planning.latest_states(rows).values())[-limit:]
    for latest in batches:
        # The state is the latest row's; the counts are the latest that say anything
        row = {**{k: v for r in rows if r["batch_id"] == latest["batch_id"] for k, v in r.items() if v is not None},
               "state": latest["state"]}
        lines.append(f"  {row['batch_id']}  {row['state']:<9} {row['engine'] or '':<6} "
                     f"bronze {row['bronze_rows'] or 0:>10,}  gold {row['gold_from'] or ''}..{row['gold_to'] or ''}")
    return lines


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="scripts/local.sh lakehouse",
                                     description="Plan, run and publish clickstream batches without an orchestrator.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="process every waiting file, batch by batch, and publish each")
    run.add_argument("engine", nargs="?", choices=["auto", *planning.ENGINES], default="auto")
    shown = commands.add_parser("status", help="waiting files, table sizes and the latest batches")
    shown.add_argument("--short", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.command == "status":
        lines = status()
        print(lines[0] if args.short else "\n".join(lines))
        return 0

    from crm_pipeline.bronze_loader import snowflake_session

    run_id = f"cli-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    forced = None if args.engine == "auto" else args.engine
    while True:
        planned = plan(forced, "cli", run_id)
        if planned["action"] == "run":
            step = run_in_process if planned["engine"] == planning.PANDAS else run_on_spark
            step(planned, "cli", run_id, echo=print)
        with snowflake_session() as snowflake:
            published = publish(planned, snowflake)
        print(json.dumps({"plan": planned, "published": published}, default=str))
        if planned["action"] == "nothing":
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
