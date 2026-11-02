"""The clickstream's data contract: what both engines read, write and compute.
"""
from __future__ import annotations

from dataclasses import dataclass

# Every date in the report is a New York day: keep in step with dbt's var business_timezone and
# TIMEZONE in dags/crm_pipeline/config.py.
BUSINESS_TIMEZONE = "America/New_York"

EVENT_NAMES = (
    "page_view", "view_item_list", "view_item", "add_to_cart", "begin_checkout", "purchase",
    "scroll", "user_engagement",
)

# The funnel's steps after a session starts: the event that marks the step, and the gold column
# that counts the sessions with at least one.
FUNNEL_STEPS = (
    ("view_item", "view_item_sessions"),
    ("add_to_cart", "add_to_cart_sessions"),
    ("begin_checkout", "checkout_sessions"),
    ("purchase", "purchase_sessions"),
)

# GA4's definition of an engaged session: at least 10 seconds of engagement, at least two page
# views, or a conversion (here, a purchase).
ENGAGED_MIN_MSEC = 10_000
ENGAGED_MIN_PAGE_VIEWS = 2

DIRECT = "direct"           # the channel of a session that arrived without a utm_source
UNKNOWN_DEVICE = "unknown"  # the device of an event that did not say

# The only event_time format accepted: what the storefront's collector writes, UTC, milliseconds.
# Both engines check the pattern before parsing, so a value one parser would accept and the other
# would not is refused by both. The format is pandas' strftime and Spark's datetime pattern.
EVENT_TIME_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$"
EVENT_TIME_FORMAT_PANDAS = "%Y-%m-%dT%H:%M:%S.%fZ"
EVENT_TIME_FORMAT_SPARK = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
# Moments outside these years are refused: pandas' timestamps end in 2262, Spark's do not.
EVENT_TIME_YEARS = (2000, 2099)

# An integer field (customer_id, the ids in params) is read only when it is plain digits that fit
# a bigint; anything else is null. Casting rules differ between pandas and Spark (1e3, +5, 1.0).
# [0-9], not \d: Python's \d also matches other scripts' digits, Java's does not.
INTEGER_PATTERN = r"^-?[0-9]{1,18}$"

# The rules a bronze row must pass to reach silver, in the order they are checked. A row that
# fails one is counted under the first rule it fails and dropped (the effect of Lakeflow's
# expect_or_drop). Bronze keeps it, so a rule can be fixed and silver rebuilt.
RULES = (
    ("parsable", "the line is a JSON object"),
    ("has_ids", "event_id, session_id and brand are not empty"),
    ("known_event_name", "event_name is one of EVENT_NAMES"),
    ("valid_event_time", "event_time matches EVENT_TIME_PATTERN, is a real moment and falls in EVENT_TIME_YEARS"),
)


@dataclass(frozen=True)
class Column:
    name: str
    type: str               # Delta (Spark SQL) type: string, bigint, double, timestamp, date


@dataclass(frozen=True)
class Table:
    schema: str             # bronze, silver or gold: a Unity Catalog schema in the catalog
    name: str
    columns: tuple[Column, ...]
    comment: str = ""

    @property
    def column_names(self) -> list[str]:
        return [column.name for column in self.columns]


def _columns(spec: str) -> tuple[Column, ...]:
    return tuple(Column(*item.split()) for item in spec.split(",") if item.strip())


# The fields of an event as the collector writes them. Every one is read as text into bronze.
EVENT_FIELDS = (
    "event_id", "event_time", "event_name", "brand", "session_id", "client_id", "customer_id",
    "page_path", "device", "user_agent", "utm_source", "utm_medium", "utm_campaign", "params",
)

BRONZE_EVENTS = Table(
    "bronze", "web_events",
    _columns(", ".join(f"{field} string" for field in EVENT_FIELDS)
             + ", _corrupt_record string, _source_file string, _batch_id string, _ingested_at timestamp"),
    "Storefront clickstream as it landed: one row per line of every export file, every field as "
    "text. _corrupt_record holds a line that is not a JSON object. Append-only.",
)

SILVER_EVENTS = Table(
    "silver", "web_events",
    _columns(
        "event_id string, event_time timestamp, business_date date, event_name string, brand string, "
        "session_id string, client_id string, customer_id bigint, page_path string, device string, "
        "channel string, utm_medium string, utm_campaign string, variant_id bigint, order_number string, "
        "engagement_time_msec bigint, _batch_id string"
    ),
    "One row per event_id that passed the RULES. channel is the lower-cased utm_source, or 'direct'. "
    "_batch_id is the batch that inserted the row.",
)

GOLD_FUNNEL = Table(
    "gold", "web_daily_funnel",
    _columns(
        "brand string, business_date date, channel string, device string, sessions bigint, "
        "engaged_sessions bigint, view_item_sessions bigint, add_to_cart_sessions bigint, "
        "checkout_sessions bigint, purchase_sessions bigint, purchases bigint, events bigint, "
        "_batch_id string, _engine string"
    ),
    "Sessions per brand, New York day, channel and device, by the step of the funnel they reached. "
    "A session counts on the day, channel and device of its first event. _batch_id and _engine say "
    "which batch last rebuilt the day, and on which engine.",
)

# What the planner decided and what the engine did, one row per state a batch reaches. The batch's
# files are listed in its PLANNED row, so both engines read the same list from the same table.
BATCH_LEDGER = Table(
    "bronze", "web_event_batches",
    _columns(
        "batch_id string, state string, recorded_at timestamp, engine string, reason string, "
        "files bigint, compressed_bytes bigint, estimated_rows bigint, estimated_pandas_bytes bigint, "
        "budget_bytes bigint, files_json string, contract string, orchestrator string, run_id string, "
        "bronze_rows bigint, silver_inserted bigint, dropped_json string, gold_from date, gold_to date, "
        "gold_rows bigint, observed_pandas_bytes bigint, seconds double, error string"
    ),
    "The clickstream batches: PLANNED by the orchestrator with the engine the gate chose and why, "
    "RUNNING and SUCCEEDED (or FAILED) by the engine, PUBLISHED once gold reached the warehouse.",
)

PLANNED, RUNNING, SUCCEEDED, FAILED, PUBLISHED = "PLANNED", "RUNNING", "SUCCEEDED", "FAILED", "PUBLISHED"

# The tables the engines write, in the order a batch writes them.
TABLES = (BRONZE_EVENTS, SILVER_EVENTS, GOLD_FUNNEL)

# Gold columns that say who built a row rather than what happened: they differ between engines.
LINEAGE_COLUMNS = ("_batch_id", "_engine", "_ingested_at")

# A session lasts less than a day (the collector starts a new one after 30 minutes without an
# event), so the sessions that start on a day have all their events between the day before and
# the day after. Gold is rebuilt from silver read over that span.
SESSION_SPAN_DAYS = 1


def table_properties(table: Table) -> dict[str, str]:
    """The properties every engine creates a table with: the protocol both writers support (no
    deletion vectors, which delta-rs cannot write; no column mapping or liquid clustering either),
    and append-only for bronze and the ledger, which both writers enforce."""
    properties = {"delta.enableDeletionVectors": "false"}
    if table in (BRONZE_EVENTS, BATCH_LEDGER):
        properties["delta.appendOnly"] = "true"
    return properties
