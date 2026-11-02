"""The in-process engine: one batch of clickstream files, with pandas and delta-rs.
"""
from __future__ import annotations

import gzip
import io
import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from crm_lakehouse import contract_fingerprint, delta_io, planning
from crm_lakehouse.contract import (
    BRONZE_EVENTS, BUSINESS_TIMEZONE, DIRECT, ENGAGED_MIN_MSEC, ENGAGED_MIN_PAGE_VIEWS, EVENT_FIELDS,
    EVENT_NAMES, EVENT_TIME_FORMAT_PANDAS, EVENT_TIME_PATTERN, EVENT_TIME_YEARS, FAILED, FUNNEL_STEPS,
    GOLD_FUNNEL, INTEGER_PATTERN, RULES, RUNNING, SESSION_SPAN_DAYS, SILVER_EVENTS, SUCCEEDED,
    UNKNOWN_DEVICE,
)
from crm_lakehouse.layout import Lakehouse

log = logging.getLogger(__name__)

ENGINE = planning.PANDAS
SESSION_KEYS = ["brand", "session_id"]
GOLD_KEYS = ["brand", "business_date", "channel", "device"]


# --- bronze --------------------------------------------------------------------------------

class _Number(str):
    """A JSON number as it was written. Spark copies number literals verbatim (1.50, 1e3) where
    Python would spell them again (1.5, 1000.0)."""


_DECODER = json.JSONDecoder(parse_int=_Number, parse_float=_Number, parse_constant=_Number)


def _dump(value) -> str:
    """Compact JSON, as Jackson writes a value Spark reads into a string column."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, _Number):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, list):
        return "[" + ",".join(_dump(item) for item in value) + "]"
    return "{" + ",".join(f"{json.dumps(key, ensure_ascii=False)}:{_dump(item)}" for key, item in value.items()) + "}"


def _text(value):
    """A JSON value as the text Spark reads into a string column: a string or a number as written,
    anything else as compact JSON."""
    if value is None or isinstance(value, str):
        return None if value is None else str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return _dump(value)


def parse_line(line: str) -> list[dict]:
    """The bronze rows of one line, as Spark's JSON reader in PERMISSIVE mode makes them: the
    first JSON value on the line (text after it is ignored, as Jackson does); a line that is not
    an object (or an array of objects) becomes one row holding the line in _corrupt_record."""
    stripped = line.lstrip()
    if not stripped.strip():
        return []                                   # blank lines are skipped
    try:
        value, _ = _DECODER.raw_decode(stripped)
    except ValueError:
        return [{"_corrupt_record": line}]
    objects = value if isinstance(value, list) else [value]
    if not objects or not all(isinstance(item, dict) for item in objects):
        return [{"_corrupt_record": line}]
    return [{field: _text(item.get(field)) for field in EVENT_FIELDS} for item in objects]


def read_files(lakehouse: Lakehouse, paths: list[str], batch_id: str, ingested_at: datetime) -> pd.DataFrame:
    rows = []
    for path in paths:
        with lakehouse.files.open(path) as stored:
            raw = gzip.GzipFile(fileobj=stored) if path.endswith(".gz") else stored
            # Universal newlines, as Spark splits lines on \n, \r\n and \r
            with io.TextIOWrapper(raw, encoding="utf-8") as lines:
                for line in lines:
                    for row in parse_line(line.rstrip("\n")):
                        row["_source_file"] = path
                        rows.append(row)
    frame = pd.DataFrame.from_records(rows, columns=list(EVENT_FIELDS) + ["_corrupt_record", "_source_file"])
    frame = frame.astype("object").where(frame.notna(), None)
    frame["_batch_id"] = batch_id
    frame["_ingested_at"] = pd.Timestamp(ingested_at)
    return frame


# --- silver --------------------------------------------------------------------------------

def _not_empty(values: pd.Series) -> pd.Series:
    return values.notna() & (values.fillna("") != "")


def _trim_lower(values: pd.Series) -> pd.Series:
    """Spark's lower(trim(x)): trim removes spaces only."""
    return values.map(lambda v: v.strip(" ").lower() if isinstance(v, str) else None)


def parse_times(values: pd.Series) -> pd.Series:
    shaped = values.map(lambda v: isinstance(v, str)) & values.fillna("").str.match(EVENT_TIME_PATTERN)
    times = pd.to_datetime(values.where(shaped), format=EVENT_TIME_FORMAT_PANDAS, utc=True, errors="coerce")
    first, last = EVENT_TIME_YEARS
    return times.where((times.dt.year >= first) & (times.dt.year <= last))


def to_integers(values: pd.Series) -> pd.Series:
    plain = values.fillna("").astype(str).str.fullmatch(INTEGER_PATTERN)
    return pd.array([int(v) if ok else None for v, ok in zip(values, plain)], dtype="Int64")


def _param_text(params, name: str):
    """get_json_object(params, '$.<name>'): the field of a JSON object, as text. A number in params
    is read as an integer only (variant_id, engagement_time_msec), so how the two engines would
    spell a decimal does not matter."""
    if not isinstance(params, str):
        return None
    try:
        value = _DECODER.decode(params)
    except ValueError:
        return None
    return _text(value.get(name)) if isinstance(value, dict) else None


def to_silver(bronze: pd.DataFrame, batch_id: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """The batch's rows that pass the RULES, one per event_id, as silver rows; and how many rows
    each rule dropped, plus the duplicates."""
    failed = pd.Series([None] * len(bronze), index=bronze.index, dtype="object")
    times = parse_times(bronze["event_time"])
    checks = {
        "parsable": bronze["_corrupt_record"].isna(),
        "has_ids": _not_empty(bronze["event_id"]) & _not_empty(bronze["session_id"])
        & _not_empty(_trim_lower(bronze["brand"])),
        "known_event_name": bronze["event_name"].isin(EVENT_NAMES),
        "valid_event_time": times.notna(),
    }
    for rule, _ in RULES:
        failed[failed.isna() & ~checks[rule]] = rule
    dropped = {rule: int((failed == rule).sum()) for rule, _ in RULES}

    valid = bronze[failed.isna()].assign(_time=times[failed.isna()])
    # One row per event_id. Re-sent events are identical; when two lines share an id but not their
    # content, both engines keep the same one.
    valid = valid.sort_values(["event_id", "_source_file", "event_time", "event_name", "session_id", "params"],
                              na_position="last", kind="stable")
    unique = valid.drop_duplicates("event_id", keep="first")
    dropped["duplicates"] = len(valid) - len(unique)

    local = unique["_time"].dt.tz_convert(BUSINESS_TIMEZONE)
    channel = _trim_lower(unique["utm_source"])
    device = _trim_lower(unique["device"])
    silver = pd.DataFrame({
        "event_id": unique["event_id"],
        "event_time": unique["_time"],
        "business_date": local.dt.date,
        "event_name": unique["event_name"],
        "brand": _trim_lower(unique["brand"]),
        "session_id": unique["session_id"],
        "client_id": unique["client_id"],
        "customer_id": to_integers(unique["customer_id"]),
        "page_path": unique["page_path"],
        "device": device.where(_not_empty(device), UNKNOWN_DEVICE),
        "channel": channel.where(_not_empty(channel), DIRECT),
        "utm_medium": unique["utm_medium"],
        "utm_campaign": unique["utm_campaign"],
        "variant_id": to_integers(unique["params"].map(lambda p: _param_text(p, "variant_id"))),
        "order_number": unique["params"].map(lambda p: _param_text(p, "order_number")),
        "engagement_time_msec": to_integers(unique["params"].map(lambda p: _param_text(p, "engagement_time_msec"))),
        "_batch_id": batch_id,
    })
    return silver.reset_index(drop=True), dropped


# --- gold ----------------------------------------------------------------------------------

def build_gold(events: pd.DataFrame, first: date, last: date, batch_id: str, engine: str = ENGINE) -> pd.DataFrame:
    """The funnel of the sessions that started between first and last. events must hold every
    event of those sessions: silver read from the day before first to the day after last."""
    columns = GOLD_FUNNEL.column_names
    if events.empty:
        return pd.DataFrame(columns=columns)
    ordered = events.sort_values(SESSION_KEYS + ["event_time", "event_id"], kind="stable")
    starts = ordered.drop_duplicates(SESSION_KEYS, keep="first")[SESSION_KEYS + ["business_date", "channel", "device"]]
    flags = ordered.assign(
        page_view=ordered["event_name"].eq("page_view"),
        purchase=ordered["event_name"].eq("purchase"),
        engagement=ordered["engagement_time_msec"].fillna(0).astype("int64"),
        **{column: ordered["event_name"].eq(name) for name, column in FUNNEL_STEPS},
    )
    per_session = flags.groupby(SESSION_KEYS, sort=False).agg(
        events=("event_id", "size"),
        page_views=("page_view", "sum"),
        purchases=("purchase", "sum"),
        engagement=("engagement", "sum"),
        **{column: (column, "max") for _, column in FUNNEL_STEPS},
    ).reset_index()
    sessions = starts.merge(per_session, on=SESSION_KEYS)
    sessions = sessions[(sessions["business_date"] >= first) & (sessions["business_date"] <= last)]
    if sessions.empty:
        return pd.DataFrame(columns=columns)
    sessions = sessions.assign(engaged=(sessions["engagement"] >= ENGAGED_MIN_MSEC)
                               | (sessions["page_views"] >= ENGAGED_MIN_PAGE_VIEWS) | (sessions["purchases"] > 0))
    gold = sessions.groupby(GOLD_KEYS, sort=True).agg(
        sessions=("session_id", "size"),
        engaged_sessions=("engaged", "sum"),
        **{column: (column, "sum") for _, column in FUNNEL_STEPS},
        purchases=("purchases", "sum"),
        events=("events", "sum"),
    ).reset_index()
    gold["_batch_id"] = batch_id
    gold["_engine"] = engine
    return gold[columns]


def _between(first: date, last: date) -> str:
    return f"business_date BETWEEN {delta_io.literal(first)} AND {delta_io.literal(last)}"


def session_days(lakehouse: Lakehouse, clean: pd.DataFrame) -> tuple[date, date] | None:
    """The first and last New York day the batch's sessions started on, from silver: a session
    the batch only finished started on an earlier day, in an earlier batch."""
    if clean.empty:
        return None
    span = timedelta(days=SESSION_SPAN_DAYS)
    first, last = min(clean["business_date"]) - span, max(clean["business_date"])
    held = delta_io.read(lakehouse, SILVER_EVENTS, _between(first, last),
                         columns=SESSION_KEYS + ["business_date"]).to_pandas()
    batch_sessions = clean[SESSION_KEYS].drop_duplicates()
    held = held.merge(batch_sessions, on=SESSION_KEYS)
    starts = held.groupby(SESSION_KEYS)["business_date"].min()
    return min(starts), max(starts)


def rebuild_gold(lakehouse: Lakehouse, first: date, last: date, batch_id: str) -> int:
    span = timedelta(days=SESSION_SPAN_DAYS)
    events = delta_io.read(lakehouse, SILVER_EVENTS, _between(first - span, last + span)).to_pandas()
    gold = build_gold(events, first, last, batch_id)
    delta_io.replace_days(lakehouse, GOLD_FUNNEL, delta_io.to_arrow(gold, GOLD_FUNNEL), first, last)
    return len(gold)


# --- a batch -------------------------------------------------------------------------------

def run_batch(lakehouse: Lakehouse, batch_id: str, orchestrator: str = "", run_id: str = "") -> dict:
    """Process the planned batch; returns its SUCCEEDED ledger row."""
    started = time.monotonic()
    planned = planning.planned_row(lakehouse, batch_id)
    common = dict(engine=ENGINE, orchestrator=orchestrator, run_id=run_id, contract=contract_fingerprint())
    planning.record(lakehouse, batch_id, RUNNING, **common)
    try:
        paths = planning.batch_files(planned)
        held = planning.ingested_among(lakehouse, paths)      # a failed attempt's bronze rows
        frame = read_files(lakehouse, [path for path in paths if path not in held], batch_id,
                           datetime.now(timezone.utc))
        if len(frame):
            delta_io.append(lakehouse, BRONZE_EVENTS, delta_io.to_arrow(frame, BRONZE_EVENTS))
        if held:
            frame = delta_io.read(lakehouse, BRONZE_EVENTS, f"_batch_id = {delta_io.literal(batch_id)}").to_pandas()
        observed = int(frame.memory_usage(deep=True).sum())

        clean, dropped = to_silver(frame, batch_id)
        del frame
        inserted = delta_io.merge_insert_new(lakehouse, SILVER_EVENTS, delta_io.to_arrow(clean, SILVER_EVENTS),
                                             keys=("event_id", "business_date"))
        days = session_days(lakehouse, clean)
        gold_rows = rebuild_gold(lakehouse, *days, batch_id) if days else 0
        bronze_rows = len(clean) + sum(dropped.values())
        result = planning.record(
            lakehouse, batch_id, SUCCEEDED, **common,
            files=planned["files"], compressed_bytes=planned["compressed_bytes"], bronze_rows=bronze_rows,
            silver_inserted=inserted, dropped_json=json.dumps(dropped), gold_from=days[0] if days else None,
            gold_to=days[1] if days else None, gold_rows=gold_rows, observed_pandas_bytes=observed,
            seconds=round(time.monotonic() - started, 3),
        )
    except BaseException as exc:
        try:
            planning.record(lakehouse, batch_id, FAILED, **common, error=f"{type(exc).__name__}: {exc}"[:2000],
                            seconds=round(time.monotonic() - started, 3))
        except Exception:                       # the failure itself is what matters
            log.exception("Could not record the failure of batch %s", batch_id)
        raise
    log.info("Batch %s on %s: %s bronze rows, %s new in silver, dropped %s, gold %s..%s (%s rows) in %.1f s",
             batch_id, ENGINE, bronze_rows, inserted, dropped, result["gold_from"], result["gold_to"], gold_rows,
             result["seconds"])
    return result
