"""Plan the next batch of clickstream files, and choose the engine that processes it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import statistics
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import pyarrow as pa

from crm_lakehouse import contract_fingerprint, delta_io
from crm_lakehouse.contract import (
    BATCH_LEDGER, BRONZE_EVENTS, FAILED, PLANNED, PUBLISHED, RUNNING, SUCCEEDED,
)
from crm_lakehouse.layout import Lakehouse

log = logging.getLogger(__name__)

PANDAS, SPARK = "pandas", "spark"
ENGINES = (PANDAS, SPARK)

LANDING_SUFFIXES = (".json.gz", ".json")
MIB = 1024 * 1024

# Measured on the generated clickstream
DEFAULT_ROWS_PER_COMPRESSED_BYTE = 1 / 43.5
DEFAULT_PANDAS_BYTES_PER_ROW = 1_300
PEAK_FACTOR = 3
PEAK_OVERHEAD_BYTES = 160 * MIB
CALIBRATION_BATCHES = 20      # the most recent batches the ratios are calibrated from


@dataclass(frozen=True)
class LandingFile:
    path: str
    size: int


@dataclass(frozen=True)
class Calibration:
    rows_per_compressed_byte: float
    pandas_bytes_per_row: float
    batches: int              # succeeded batches the ratios come from; 0: the defaults

    def describe(self) -> str:
        source = f"calibrated on {self.batches} batches" if self.batches else "default ratios"
        return (f"{1 / self.rows_per_compressed_byte:.0f} compressed bytes per row, "
                f"{self.pandas_bytes_per_row:,.0f} pandas bytes per row ({source})")


@dataclass(frozen=True)
class BatchProfile:
    files: int
    compressed_bytes: int
    estimated_rows: int
    estimated_pandas_bytes: int


@dataclass(frozen=True)
class EngineDecision:
    engine: str
    reason: str


@dataclass(frozen=True)
class Plan:
    """What the orchestrator does next. action: run (an engine processes the batch), publish
    (the batch succeeded but did not reach the warehouse yet) or nothing."""

    action: str
    batch_id: str | None = None
    engine: str | None = None
    reason: str = ""
    files: int = 0
    compressed_bytes: int = 0
    estimated_rows: int = 0
    estimated_pandas_bytes: int = 0
    budget_bytes: int = 0
    resumed: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, values: dict) -> Plan:
        return cls(**values)


# --- the landing files ---------------------------------------------------------------------

def list_landing_files(lakehouse: Lakehouse) -> list[LandingFile]:
    """Every export file in the landing volume, in path order. Files still being written (the
    writers' temporary names) are left for the next batch (layout.LocalFiles)."""
    return sorted(
        (LandingFile(path, size) for path, size in lakehouse.files.list(lakehouse.landing)
         if path.endswith(LANDING_SUFFIXES)),
        key=lambda file: file.path,
    )


def ingested_files(lakehouse: Lakehouse) -> set[str]:
    """The files whose rows are in bronze."""
    return delta_io.distinct_values(lakehouse, BRONZE_EVENTS, "_source_file")


def processed_files(rows: Iterable[dict]) -> set[str]:
    """The files of the batches that succeeded. A file with no line (an export of a day without
    traffic) leaves no row in bronze to show it was read; its batch's success does."""
    rows = list(rows)
    done = {row["batch_id"] for row in rows if row["state"] in (SUCCEEDED, PUBLISHED)}
    return {path for row in rows if row["batch_id"] in done and row["state"] == PLANNED and row["files_json"]
            for path in batch_files(row)}


def pending_files(lakehouse: Lakehouse, lister: Callable[[Lakehouse], list[LandingFile]] | None = None,
                  rows: list[dict] | None = None) -> list[LandingFile]:
    """The landing files neither in bronze nor in a batch that succeeded."""
    loaded = ingested_files(lakehouse) | processed_files(ledger(lakehouse) if rows is None else rows)
    return [file for file in (lister or list_landing_files)(lakehouse) if file.path not in loaded]


# --- the ledger ----------------------------------------------------------------------------

def ledger(lakehouse: Lakehouse) -> list[dict]:
    """Every ledger row, oldest first."""
    rows = delta_io.read(lakehouse, BATCH_LEDGER).to_pylist()
    return sorted(rows, key=lambda row: (row["recorded_at"], _STATE_ORDER.get(row["state"], 0)))


_STATE_ORDER = {PLANNED: 1, RUNNING: 2, FAILED: 3, SUCCEEDED: 3, PUBLISHED: 4}


def latest_states(rows: Iterable[dict]) -> dict[str, dict]:
    """The latest row of each batch, by batch id, in the order the batches were first planned."""
    latest: dict[str, dict] = {}
    for row in rows:
        latest[row["batch_id"]] = row          # a dict keeps the position of the first row
    return latest


def planned_row(lakehouse: Lakehouse, batch_id: str) -> dict:
    """The batch's latest PLANNED row: its engine, its files."""
    rows = [row for row in ledger(lakehouse) if row["batch_id"] == batch_id and row["state"] == PLANNED]
    if not rows:
        raise LookupError(f"Batch {batch_id} was never planned.")
    return rows[-1]


def record(lakehouse: Lakehouse, batch_id: str, state: str, **values) -> dict:
    """Append one ledger row; columns not given are null."""
    row = {column: None for column in BATCH_LEDGER.column_names}
    row.update(values, batch_id=batch_id, state=state, recorded_at=datetime.now(timezone.utc))
    unknown = set(row) - set(BATCH_LEDGER.column_names)
    if unknown:
        raise ValueError(f"Not ledger columns: {sorted(unknown)}")
    delta_io.append(lakehouse, BATCH_LEDGER, pa.Table.from_pylist([row], schema=delta_io.arrow_schema(BATCH_LEDGER)))
    return row


# --- the gate ------------------------------------------------------------------------------

def calibrate(rows: Iterable[dict]) -> Calibration:
    """The ratios measured on the most recent succeeded batches, or the defaults."""
    done = [row for row in rows if row["state"] == SUCCEEDED and row["compressed_bytes"] and row["bronze_rows"]]
    done = done[-CALIBRATION_BATCHES:]
    if not done:
        return Calibration(DEFAULT_ROWS_PER_COMPRESSED_BYTE, DEFAULT_PANDAS_BYTES_PER_ROW, 0)
    rows_per_byte = sum(row["bronze_rows"] for row in done) / sum(row["compressed_bytes"] for row in done)
    in_pandas = [row["observed_pandas_bytes"] / row["bronze_rows"] for row in done if row["observed_pandas_bytes"]]
    bytes_per_row = statistics.median(in_pandas) if in_pandas else DEFAULT_PANDAS_BYTES_PER_ROW
    return Calibration(rows_per_byte, bytes_per_row, len(done))


def profile(files: list[LandingFile], calibration: Calibration) -> BatchProfile:
    compressed = sum(file.size for file in files)
    rows = round(compressed * calibration.rows_per_compressed_byte)
    peak = PEAK_OVERHEAD_BYTES + rows * calibration.pandas_bytes_per_row * PEAK_FACTOR
    return BatchProfile(len(files), compressed, rows, round(peak))


def choose_engine(batch: BatchProfile, budget_bytes: int, forced: str | None = None,
                  calibration: Calibration | None = None) -> EngineDecision:
    """pandas when the batch's estimated peak in pandas fits the in-process budget, Spark otherwise.
    forced (the run's engine parameter) overrides the estimate, and says so."""
    size = (f"{batch.files} files, {batch.compressed_bytes / MIB:,.1f} MiB compressed, "
            f"~{batch.estimated_rows:,} rows, ~{batch.estimated_pandas_bytes / MIB:,.0f} MiB at its peak in pandas")
    how = f"; {calibration.describe()}" if calibration else ""
    if forced:
        if forced not in ENGINES:
            raise ValueError(f"Unknown engine {forced!r}: use one of {', '.join(ENGINES)}.")
        return EngineDecision(forced, f"forced by the run's engine parameter ({size}{how})")
    budget = f"{budget_bytes / MIB:,.0f} MiB"
    if batch.estimated_pandas_bytes <= budget_bytes:
        return EngineDecision(PANDAS, f"fits in process: {size}, within the {budget} budget{how}")
    return EngineDecision(SPARK, f"too big for pandas: {size}, over the {budget} budget{how}")


# --- the plan ------------------------------------------------------------------------------

def batch_id_for(files: list[LandingFile], now: datetime) -> str:
    digest = hashlib.sha256("\n".join(file.path for file in files).encode()).hexdigest()[:8]
    return f"{now:%Y%m%dT%H%M%SZ}-{digest}"


def plan_batch(lakehouse: Lakehouse, budget_bytes: int, forced: str | None = None, orchestrator: str = "",
               run_id: str = "", lister: Callable[[Lakehouse], list[LandingFile]] | None = None,
               now: datetime | None = None) -> Plan:
    """The next thing to do, recorded in the ledger. An unfinished batch comes first: one that
    succeeded is published, one that never started runs on its engine, one whose in-process
    attempt did not finish moves to Spark, and one that failed on Spark runs there again."""
    now = now or datetime.now(timezone.utc)
    rows = ledger(lakehouse)
    calibration = calibrate(rows)
    common = dict(orchestrator=orchestrator, run_id=run_id, contract=contract_fingerprint(), budget_bytes=budget_bytes)

    for batch_id, latest in latest_states(rows).items():
        if latest["state"] == PUBLISHED:
            continue
        planned = [row for row in rows if row["batch_id"] == batch_id and row["state"] == PLANNED][-1]
        sizes = {key: planned[key] for key in ("files", "compressed_bytes", "estimated_rows", "estimated_pandas_bytes")}
        if latest["state"] == SUCCEEDED:
            return Plan("publish", batch_id, latest["engine"], f"{batch_id} succeeded on {latest['engine']} "
                        "but was not published yet", budget_bytes=budget_bytes, resumed=True, **sizes)
        engine, reason = planned["engine"], f"resuming {batch_id}, which did not finish"
        if forced:
            if forced != engine:
                engine, reason = forced, f"{reason}, on {forced} as the run's engine parameter asks"
        elif latest["state"] in (RUNNING, FAILED) and engine == PANDAS:
            engine = SPARK
            reason = (f"escalated: the in-process attempt at {batch_id} did not finish "
                      f"({latest['error'] or 'the worker stopped while it ran, e.g. out of memory'})")
        if latest["state"] != PLANNED or engine != planned["engine"]:
            record(lakehouse, batch_id, PLANNED, engine=engine, reason=reason,
                   files_json=planned["files_json"], **sizes, **common)
        log.info("Batch %s: %s", batch_id, reason)
        return Plan("run", batch_id, engine, reason, budget_bytes=budget_bytes, resumed=True, **sizes)

    files = pending_files(lakehouse, lister, rows)
    if not files:
        return Plan("nothing", reason="no landing file is waiting for bronze", budget_bytes=budget_bytes)
    batch = profile(files, calibration)
    decision = choose_engine(batch, budget_bytes, forced, calibration)
    batch_id = batch_id_for(files, now)
    sizes = asdict(batch)
    record(lakehouse, batch_id, PLANNED, engine=decision.engine, reason=decision.reason,
           files_json=json.dumps([[file.path, file.size] for file in files]), **sizes, **common)
    log.info("Batch %s on %s: %s", batch_id, decision.engine, decision.reason)
    return Plan("run", batch_id, decision.engine, decision.reason, budget_bytes=budget_bytes, **sizes)


def batch_files(planned: dict) -> list[str]:
    return [path for path, _ in json.loads(planned["files_json"])]


def ingested_among(lakehouse: Lakehouse, paths: list[str]) -> set[str]:
    """Which of these files bronze already holds: a retried batch skips them."""
    if not paths:
        return set()
    listed = ", ".join(delta_io.literal(path) for path in paths)
    return delta_io.distinct_values(lakehouse, BRONZE_EVENTS, "_source_file", f"_source_file IN ({listed})")
