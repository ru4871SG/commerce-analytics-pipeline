"""The Spark engine: one batch of clickstream files, with Spark and Delta Lake.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from crm_lakehouse import contract_fingerprint
from crm_lakehouse.contract import (
    BATCH_LEDGER, BRONZE_EVENTS, BUSINESS_TIMEZONE, DIRECT, ENGAGED_MIN_MSEC, ENGAGED_MIN_PAGE_VIEWS,
    EVENT_FIELDS, EVENT_NAMES, EVENT_TIME_FORMAT_SPARK, EVENT_TIME_PATTERN, EVENT_TIME_YEARS, FAILED,
    FUNNEL_STEPS, GOLD_FUNNEL, INTEGER_PATTERN, PLANNED, RULES, RUNNING, SESSION_SPAN_DAYS, SILVER_EVENTS,
    SUCCEEDED, UNKNOWN_DEVICE, Table, table_properties,
)
from crm_lakehouse.layout import Lakehouse

log = logging.getLogger(__name__)

ENGINE = "spark"
SESSION_KEYS = ["brand", "session_id"]
GOLD_KEYS = ["brand", "business_date", "channel", "device"]

# Every event field is read as text; a line that is not a JSON object lands in _corrupt_record.
JSON_SCHEMA = StructType([StructField(field, StringType()) for field in EVENT_FIELDS]
                         + [StructField("_corrupt_record", StringType())])


class ContractMismatch(RuntimeError):
    """The orchestrator planned the batch with a different release of crm_lakehouse."""


# --- tables --------------------------------------------------------------------------------

def ddl(table: Table) -> str:
    return ", ".join(f"{column.name} {column.type.upper()}" for column in table.columns)


TABLES_TO_CREATE = (BRONZE_EVENTS, SILVER_EVENTS, GOLD_FUNNEL, BATCH_LEDGER)


def ensure_tables(spark: SparkSession, lakehouse: Lakehouse) -> None:
    """Create the shared tables when they are missing, with the contract's schema and properties.
    On Databricks they are external tables at storage_root, registered in Unity Catalog."""
    for table in TABLES_TO_CREATE:
        properties = ", ".join(f"'{key}' = '{value}'" for key, value in table_properties(table).items())
        comment = table.comment.replace("'", "\\'")
        if lakehouse.is_local:
            target, location = f"delta.`{lakehouse.uri(table)}`", ""
        else:
            target, location = lakehouse.name(table), f" LOCATION '{lakehouse.uri(table)}'"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({ddl(table)}) USING DELTA COMMENT '{comment}'"
                  f"{location} TBLPROPERTIES ({properties})")


def read(spark: SparkSession, lakehouse: Lakehouse, table: Table) -> DataFrame:
    if lakehouse.is_local:
        return spark.read.format("delta").load(lakehouse.uri(table))
    return spark.read.table(lakehouse.name(table))


def delta_table(spark: SparkSession, lakehouse: Lakehouse, table: Table) -> DeltaTable:
    if lakehouse.is_local:
        return DeltaTable.forPath(spark, lakehouse.uri(table))
    return DeltaTable.forName(spark, lakehouse.name(table))


def write(frame: DataFrame, lakehouse: Lakehouse, table: Table, mode: str, **options) -> None:
    writer = frame.select(*table.column_names).write.format("delta").mode(mode).options(**options)
    if lakehouse.is_local:
        writer.save(lakehouse.uri(table))
    else:
        writer.saveAsTable(lakehouse.name(table))


# --- the ledger ----------------------------------------------------------------------------

def planned_row(spark: SparkSession, lakehouse: Lakehouse, batch_id: str) -> dict:
    rows = (read(spark, lakehouse, BATCH_LEDGER)
            .filter((F.col("batch_id") == batch_id) & (F.col("state") == PLANNED))
            .orderBy(F.col("recorded_at").desc()).limit(1).collect())
    if not rows:
        raise LookupError(f"Batch {batch_id} was never planned.")
    return rows[0].asDict()


def record(spark: SparkSession, lakehouse: Lakehouse, batch_id: str, state: str, **values) -> dict:
    row = {column: None for column in BATCH_LEDGER.column_names}
    row.update(values, batch_id=batch_id, state=state, recorded_at=datetime.now(timezone.utc))
    data = spark.createDataFrame([tuple(row[name] for name in BATCH_LEDGER.column_names)], ddl(BATCH_LEDGER))
    write(data, lakehouse, BATCH_LEDGER, "append")
    return row


# --- bronze --------------------------------------------------------------------------------

def source_path(column: Column) -> Column:
    """_metadata.file_path is a URI (file:/..., dbfs:/Volumes/...); bronze keeps the plain path the
    planner listed, as the in-process engine does."""
    return F.regexp_replace(column, r"^(file|dbfs):(//)?", "")


def load_bronze(spark: SparkSession, lakehouse: Lakehouse, paths: list[str], batch_id: str) -> int:
    """Append the lines of the files that bronze does not hold yet. Returns the rows appended."""
    held = set()
    if paths:
        held = {row[0] for row in read(spark, lakehouse, BRONZE_EVENTS)
                .filter(F.col("_source_file").isin(paths)).select("_source_file").distinct().collect()}
    new = [path for path in paths if path not in held]
    if not new:
        return 0
    lines = (spark.read.schema(JSON_SCHEMA)
             .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", "_corrupt_record")
             .json(new))
    rows = lines.select(
        *EVENT_FIELDS, "_corrupt_record",
        source_path(F.col("_metadata.file_path")).alias("_source_file"),
        F.lit(batch_id).alias("_batch_id"),
        F.current_timestamp().alias("_ingested_at"),
    )
    write(rows, lakehouse, BRONZE_EVENTS, "append")
    # Every row must name a file of the batch: a path the planner did not list would never be
    # recognised as loaded, and its rows would be loaded again
    files = {row[0] for row in read(spark, lakehouse, BRONZE_EVENTS)
             .filter(F.col("_batch_id") == batch_id).select("_source_file").distinct().collect()}
    if not files <= set(paths):
        raise RuntimeError(f"Bronze names files the batch did not list: {sorted(files - set(paths))[:5]}")
    return len(new)


# --- silver --------------------------------------------------------------------------------

def _not_empty(column: Column) -> Column:
    return F.coalesce(column, F.lit("")) != ""


def _trim_lower(column: Column) -> Column:
    return F.lower(F.trim(column))


def _integer(text: Column) -> Column:
    return F.when(text.rlike(INTEGER_PATTERN), text.try_cast("bigint"))


def to_silver(bronze: DataFrame, batch_id: str) -> tuple[DataFrame, dict[str, int]]:
    """The batch's rows that pass the RULES, one per event_id; and the rows each rule dropped."""
    time_text = F.col("event_time")
    parsed = F.when(time_text.rlike(EVENT_TIME_PATTERN),
                    F.try_to_timestamp(time_text, F.lit(EVENT_TIME_FORMAT_SPARK)))
    first, last = EVENT_TIME_YEARS
    checks = {
        "parsable": F.col("_corrupt_record").isNull(),
        "has_ids": _not_empty(F.col("event_id")) & _not_empty(F.col("session_id"))
        & _not_empty(_trim_lower(F.col("brand"))),
        "known_event_name": F.col("event_name").isin(*EVENT_NAMES),
        "valid_event_time": parsed.isNotNull() & F.year(parsed).between(first, last),
    }
    failed = F.lit(None).cast("string")
    for rule, _ in reversed(RULES):       # the first rule a row fails is the one it counts under
        failed = F.when(~F.coalesce(checks[rule], F.lit(False)), F.lit(rule)).otherwise(failed)
    checked = bronze.withColumn("_failed", failed).withColumn("_time", parsed)
    counts = {row["_failed"]: row["count"] for row in checked.groupBy("_failed").count().collect()}
    dropped = {rule: int(counts.get(rule, 0)) for rule, _ in RULES}

    order = [F.col(name).asc_nulls_last() for name in ("_source_file", "event_time", "event_name", "session_id", "params")]
    valid = checked.filter(F.col("_failed").isNull())
    unique = (valid.withColumn("_rank", F.row_number().over(Window.partitionBy("event_id").orderBy(*order)))
              .filter(F.col("_rank") == 1))
    dropped["duplicates"] = int(counts.get(None, 0)) - unique.count()

    channel, device = _trim_lower(F.col("utm_source")), _trim_lower(F.col("device"))
    silver = unique.select(
        "event_id",
        F.col("_time").alias("event_time"),
        F.to_date(F.from_utc_timestamp(F.col("_time"), BUSINESS_TIMEZONE)).alias("business_date"),
        "event_name",
        _trim_lower(F.col("brand")).alias("brand"),
        "session_id", "client_id",
        _integer(F.col("customer_id")).alias("customer_id"),
        "page_path",
        F.coalesce(F.nullif(device, F.lit("")), F.lit(UNKNOWN_DEVICE)).alias("device"),
        F.coalesce(F.nullif(channel, F.lit("")), F.lit(DIRECT)).alias("channel"),
        "utm_medium", "utm_campaign",
        _integer(F.get_json_object("params", "$.variant_id")).alias("variant_id"),
        F.get_json_object("params", "$.order_number").alias("order_number"),
        _integer(F.get_json_object("params", "$.engagement_time_msec")).alias("engagement_time_msec"),
        F.lit(batch_id).alias("_batch_id"),
    )
    return silver, dropped


def merge_silver(spark: SparkSession, lakehouse: Lakehouse, silver: DataFrame) -> int:
    """Insert the events silver does not hold yet; returns how many."""
    metrics = (delta_table(spark, lakehouse, SILVER_EVENTS).alias("t")
               .merge(silver.alias("s"), "t.event_id = s.event_id AND t.business_date = s.business_date")
               .whenNotMatchedInsertAll()
               .execute())
    return int(metrics.first()["num_inserted_rows"])


# --- gold ----------------------------------------------------------------------------------

def _flag(condition: Column) -> Column:
    return F.when(condition, 1).otherwise(0)


def build_gold(events: DataFrame, first: date, last: date, batch_id: str) -> DataFrame:
    """pandas_engine.build_gold, in Spark: the funnel of the sessions that started first..last."""
    name = F.col("event_name")
    starts = (events.withColumn("_rank", F.row_number().over(
                  Window.partitionBy(*SESSION_KEYS).orderBy("event_time", "event_id")))
              .filter(F.col("_rank") == 1)
              .select(*SESSION_KEYS, "business_date", "channel", "device"))
    per_session = events.groupBy(*SESSION_KEYS).agg(
        F.count(F.lit(1)).alias("events"),
        F.sum(_flag(name == "page_view")).alias("page_views"),
        F.sum(_flag(name == "purchase")).alias("purchases"),
        F.coalesce(F.sum("engagement_time_msec"), F.lit(0)).alias("engagement"),
        *[F.max(_flag(name == event)).alias(column) for event, column in FUNNEL_STEPS],
    )
    sessions = (starts.join(per_session, SESSION_KEYS)
                .filter(F.col("business_date").between(F.lit(first), F.lit(last)))
                .withColumn("engaged", _flag((F.col("engagement") >= ENGAGED_MIN_MSEC)
                                             | (F.col("page_views") >= ENGAGED_MIN_PAGE_VIEWS)
                                             | (F.col("purchases") > 0))))
    gold = sessions.groupBy(*GOLD_KEYS).agg(
        F.count(F.lit(1)).alias("sessions"),
        F.sum("engaged").alias("engaged_sessions"),
        *[F.sum(column).alias(column) for _, column in FUNNEL_STEPS],
        F.sum("purchases").alias("purchases"),
        F.sum("events").alias("events"),
    )
    measures = [column.name for column in GOLD_FUNNEL.columns if column.type == "bigint"]
    return gold.select(
        *GOLD_KEYS, *[F.col(column).cast("bigint").alias(column) for column in measures],
        F.lit(batch_id).alias("_batch_id"), F.lit(ENGINE).alias("_engine"),
    )


def session_days(spark: SparkSession, lakehouse: Lakehouse, clean: DataFrame) -> tuple[date, date] | None:
    """The first and last New York day the batch's sessions started on, from silver."""
    bounds = clean.agg(F.min("business_date").alias("first"), F.max("business_date").alias("last")).first()
    if bounds["first"] is None:
        return None
    first = bounds["first"] - timedelta(days=SESSION_SPAN_DAYS)
    starts = (read(spark, lakehouse, SILVER_EVENTS)
              .filter(F.col("business_date").between(F.lit(first), F.lit(bounds["last"])))
              .join(clean.select(*SESSION_KEYS).distinct(), SESSION_KEYS)
              .groupBy(*SESSION_KEYS).agg(F.min("business_date").alias("start")))
    days = starts.agg(F.min("start").alias("first"), F.max("start").alias("last")).first()
    return days["first"], days["last"]


def rebuild_gold(spark: SparkSession, lakehouse: Lakehouse, first: date, last: date, batch_id: str) -> int:
    span = timedelta(days=SESSION_SPAN_DAYS)
    events = read(spark, lakehouse, SILVER_EVENTS).filter(
        F.col("business_date").between(F.lit(first - span), F.lit(last + span)))
    gold = build_gold(events, first, last, batch_id)
    write(gold, lakehouse, GOLD_FUNNEL, "overwrite",
          replaceWhere=f"business_date >= '{first.isoformat()}' AND business_date <= '{last.isoformat()}'")
    return read(spark, lakehouse, GOLD_FUNNEL).filter(F.col("_batch_id") == batch_id).count()


# --- a batch -------------------------------------------------------------------------------

def run_batch(spark: SparkSession, lakehouse: Lakehouse, batch_id: str, orchestrator: str = "",
              run_id: str = "") -> dict:
    """Process the planned batch; returns its SUCCEEDED ledger row."""
    started = time.monotonic()
    ensure_tables(spark, lakehouse)
    planned = planned_row(spark, lakehouse, batch_id)
    common = dict(engine=ENGINE, orchestrator=orchestrator, run_id=run_id, contract=contract_fingerprint())
    if planned["contract"] != common["contract"]:
        error = (f"Batch {batch_id} was planned by crm_lakehouse {planned['contract']}, and this job runs "
                 f"{common['contract']}: deploy the same release to the orchestrator and to this job.")
        record(spark, lakehouse, batch_id, FAILED, **common, error=error)
        raise ContractMismatch(error)
    record(spark, lakehouse, batch_id, RUNNING, **common)
    try:
        paths = [path for path, _ in json.loads(planned["files_json"])]
        load_bronze(spark, lakehouse, paths, batch_id)
        batch = read(spark, lakehouse, BRONZE_EVENTS).filter(F.col("_batch_id") == batch_id)
        clean, dropped = to_silver(batch, batch_id)
        inserted = merge_silver(spark, lakehouse, clean)
        days = session_days(spark, lakehouse, clean)
        gold_rows = rebuild_gold(spark, lakehouse, *days, batch_id) if days else 0
        result = record(
            spark, lakehouse, batch_id, SUCCEEDED, **common,
            files=planned["files"], compressed_bytes=planned["compressed_bytes"], bronze_rows=batch.count(),
            silver_inserted=inserted, dropped_json=json.dumps(dropped), gold_from=days[0] if days else None,
            gold_to=days[1] if days else None, gold_rows=gold_rows, seconds=round(time.monotonic() - started, 3),
        )
    except BaseException as exc:
        try:
            record(spark, lakehouse, batch_id, FAILED, **common, error=f"{type(exc).__name__}: {exc}"[:2000],
                   seconds=round(time.monotonic() - started, 3))
        except Exception:
            log.exception("Could not record the failure of batch %s", batch_id)
        raise
    log.info("Batch %s on %s: %s bronze rows, %s new in silver, dropped %s, gold %s..%s (%s rows) in %.1f s",
             batch_id, ENGINE, result["bronze_rows"], inserted, dropped, result["gold_from"], result["gold_to"],
             gold_rows, result["seconds"])
    return result


def healthcheck(spark: SparkSession, lakehouse: Lakehouse) -> dict:
    """What the post-deployment check reads: every table answers, and which release runs."""
    import crm_lakehouse

    return {
        "contract": contract_fingerprint(),
        "package": crm_lakehouse.__file__,        # inside the deployed wheel, not a source tree
        "spark": spark.version,
        "session_time_zone": spark.conf.get("spark.sql.session.timeZone"),
        "tables": {lakehouse.name(table): read(spark, lakehouse, table).limit(1).count() >= 0
                   for table in TABLES_TO_CREATE},
        "ledger_batches": read(spark, lakehouse, BATCH_LEDGER).select("batch_id").distinct().count(),
    }
