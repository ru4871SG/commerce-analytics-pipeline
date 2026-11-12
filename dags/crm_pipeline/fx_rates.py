"""Daily exchange rates from the European Central Bank, loaded beside the CRM.
"""
from __future__ import annotations

import csv
import io
import logging
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests

from crm_pipeline import bronze_loader, connections
from crm_pipeline.config import ECB_FX_CONN_ID, FX_RATES_SEED, load_brands
from crm_pipeline.errors import PermanentError
from crm_pipeline.raw_tables import FX_RATES_TABLE, create_side_tables

log = logging.getLogger(__name__)

BASE_URL = "https://data-api.ecb.europa.eu"
ENDPOINT = "/service/data/EXR"
# What the API's CSV starts with when asked for detail=dataonly. Any other header is not the
# answer this module knows how to read
HEADER = ["KEY", "FREQ", "CURRENCY", "CURRENCY_DENOM", "EXR_TYPE", "EXR_SUFFIX", "TIME_PERIOD", "OBS_VALUE"]
USER_AGENT = "commerce-analytics-pipeline fx-rates loader"
TIMEOUT_SECONDS = 30
TRIES = 3
BACKOFF_SECONDS = (2, 8)            # the waits before the second and the third try
SEED_START = date(2025, 1, 1)       # the first day of shop data (START in generate_fake_crm.py)
INSERT_BATCH_ROWS = 500


@dataclass(frozen=True)
class Rate:
    rate_date: date
    currency_code: str
    eur_rate: Decimal               # units of the currency per euro, as the ECB published it


class RatesUnavailable(Exception):
    """The API did not answer, or answered that it cannot right now. Trying again can succeed."""


def currencies(brands=None) -> tuple[str, ...]:
    """The currencies to ask for: every brand's, and USD, which they are converted to. The
    euro is what every rate is measured in, so it has no rate of its own."""
    codes = {brand.currency_code.upper() for brand in (load_brands() if brands is None else brands)} | {"USD"}
    return tuple(sorted(codes - {"EUR"}))


def series_url(codes: tuple[str, ...], base_url: str = BASE_URL, endpoint: str = ENDPOINT) -> str:
    """The daily (D) spot (SP00) reference rates (A) of these currencies against the euro."""
    return f"{base_url.rstrip('/')}{endpoint.rstrip('/')}/D.{'+'.join(codes)}.EUR.SP00.A"


def fetch_csv(url: str, params: dict, timeout: int = TIMEOUT_SECONDS) -> str:
    """One GET. A window with no rate in it (a weekend) answers 200 with an empty body."""
    response = requests.get(url, params=params, headers={"User-Agent": USER_AGENT, "Accept": "text/csv"},
                            timeout=timeout)
    if response.status_code == 200:
        return response.text
    if 400 <= response.status_code < 500 and response.status_code != 429:
        # A series that does not exist, a bad period: asking again gives the same answer
        raise PermanentError(f"The ECB API refused the request ({response.status_code}): {response.text[:300]}")
    raise RatesUnavailable(f"The ECB API answered {response.status_code}: {response.text[:200]}")


def fetch_with_retries(fetch: Callable[[str, dict], str], url: str, params: dict,
                       sleep: Callable[[float], None] = time.sleep) -> str:
    """Up to three tries, with a wait between them. After the third the step fails for good: the
    next run asks for the same days again, so nothing is lost by stopping here."""
    for attempt in range(1, TRIES + 1):
        try:
            return fetch(url, params)
        except (requests.RequestException, RatesUnavailable) as error:
            if attempt == TRIES:
                raise PermanentError(f"The ECB API did not answer in {TRIES} tries: {error}") from error
            wait = BACKOFF_SECONDS[attempt - 1]
            log.warning("FX rates: try %s of %s failed (%s). Trying again in %s s.", attempt, TRIES, error, wait)
            sleep(wait)
    raise AssertionError("unreachable")


def parse(text: str, codes: tuple[str, ...], first: date, last: date) -> list[Rate]:
    """The rates in the API's CSV, oldest first. Strict: a header, a series, a currency, a day or
    a number that is not what was asked for means the answer is not understood, and nothing of
    it is loaded. An empty answer is no rate, which is what a weekend gives."""
    if not text.strip():
        return []
    rows = list(csv.reader(io.StringIO(text)))
    if rows[0] != HEADER:
        raise PermanentError(f"The ECB API's answer does not start with the expected header: {rows[0][:10]}")
    rates: dict[tuple[date, str], Rate] = {}
    for number, row in enumerate(rows[1:], start=2):
        if not row:
            continue
        if len(row) != len(HEADER):
            raise PermanentError(f"Line {number} of the ECB API's answer has {len(row)} fields, not {len(HEADER)}.")
        _, frequency, currency, denominator, rate_type, suffix, period, value = row
        if (frequency, denominator, rate_type, suffix) != ("D", "EUR", "SP00", "A") or currency not in codes:
            raise PermanentError(f"Line {number} of the ECB API's answer is not a series that was asked for: {row}")
        try:
            day = date.fromisoformat(period)
            rate = Decimal(value)
        except (ValueError, InvalidOperation) as error:
            raise PermanentError(f"Line {number} of the ECB API's answer has no day or no number: {row}") from error
        if not first <= day <= last:
            raise PermanentError(f"Line {number} of the ECB API's answer is of {day}, outside {first} to {last}.")
        if not rate.is_finite() or rate <= 0:
            raise PermanentError(f"Line {number} of the ECB API's answer has a rate that is not positive: {row}")
        if (day, currency) in rates:
            raise PermanentError(f"The ECB API's answer has two rates for {currency} on {day}.")
        rates[day, currency] = Rate(day, currency, rate)
    return [rates[key] for key in sorted(rates)]


def seed_latest(path: Path = FX_RATES_SEED) -> date | None:
    """The latest day the seed has a rate for; None when there is no seed."""
    if not path.exists():
        return None
    with open(path, newline="") as file:
        days = [date.fromisoformat(row["rate_date"]) for row in csv.DictReader(file)]
    return max(days, default=None)


def loaded_latest(cursor) -> date | None:
    """The latest day RAW.REF.FX_RATES has a rate for; None while it is empty."""
    cursor.execute(f"select max(RATE_DATE) from {FX_RATES_TABLE}")
    latest = cursor.fetchone()[0]
    return latest.date() if isinstance(latest, datetime) else latest


def request_window(latest: date | None, today: date) -> tuple[date, date] | None:
    """The days to ask for: those after the latest rate there is, up to today. None when that
    rate is today's."""
    first = SEED_START if latest is None else latest + timedelta(days=1)
    return (first, today) if first <= today else None


def _held(cursor) -> int:
    cursor.execute(f"select count(*) from {FX_RATES_TABLE}")
    return int(cursor.fetchone()[0])


def append(cursor, rates: list[Rate], loaded_at: datetime) -> int:
    """Insert the rates RAW.REF.FX_RATES does not hold yet: an anti-join on currency and day, so
    a rate that arrives twice (a retry, two runs on one day) is stored once. Returns how many
    were added."""
    if not rates:
        return 0
    before = _held(cursor)
    cursor.execute("begin")
    try:
        for start in range(0, len(rates), INSERT_BATCH_ROWS):
            chunk = rates[start:start + INSERT_BATCH_ROWS]
            values = ", ".join(["(%s, %s, %s)"] * len(chunk))
            cursor.execute(
                f"insert into {FX_RATES_TABLE} (RATE_DATE, CURRENCY_CODE, EUR_RATE, _LOADED_AT) "
                f"select v.rate_date::date, v.currency_code, v.eur_rate::number(18, 6), %s::timestamp_ntz "
                f"from (values {values}) as v (rate_date, currency_code, eur_rate) "
                f"where not exists (select 1 from {FX_RATES_TABLE} as held "
                f"where held.CURRENCY_CODE = v.currency_code and held.RATE_DATE = v.rate_date::date)",
                [loaded_at, *(value for rate in chunk
                              for value in (rate.rate_date.isoformat(), rate.currency_code, str(rate.eur_rate)))],
            )
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise
    return _held(cursor) - before


def _endpoint(conn_id: str) -> tuple[str, str]:
    """Where the connection says the API is: its base URL and the path of the rates."""
    extra = connections.get_connection(conn_id).extra
    return extra.get("base_url", BASE_URL), extra.get("endpoint", ENDPOINT)


def _params(first: date, last: date) -> dict:
    # dataonly: without it the same rows come with 32 columns of metadata
    return {"startPeriod": first.isoformat(), "endPeriod": last.isoformat(), "format": "csvdata", "detail": "dataonly"}


def load(open_session=bronze_loader.snowflake_session, conn_id: str = ECB_FX_CONN_ID,
         fetch: Callable[[str, dict], str] = fetch_csv, today: date | None = None,
         sleep: Callable[[float], None] = time.sleep, seed: Path = FX_RATES_SEED) -> dict:
    """Ask the ECB for the days after the latest rate there is, and append them to
    RAW.REF.FX_RATES. Returns what happened: state is SKIPPED (no connection), UP_TO_DATE
    (nothing to ask for, or nothing new in the answer) or LOADED."""
    try:
        base_url, endpoint = _endpoint(conn_id)
    except connections.ConnectionNotFound:
        # Any other error (a secrets backend that is down) fails the step: it is never skipped quietly
        reason = (f"No {conn_id} connection: no rates are asked for. dbt has the seed's and those loaded "
                  "before, and carries the last one forward.")
        log.info("FX rates: skipped. %s", reason)
        return {"state": "SKIPPED", "reason": reason, "added": 0}

    today = today or datetime.now(timezone.utc).date()
    codes = currencies()
    with open_session() as connection:
        cursor = connection.cursor()
        try:
            create_side_tables(cursor)
            latest = max((day for day in (seed_latest(seed), loaded_latest(cursor)) if day is not None), default=None)
            window = request_window(latest, today)
            if window is None:
                log.info("FX rates: up to date, nothing to ask for. The latest rate is of %s.", latest)
                return {"state": "UP_TO_DATE", "added": 0, "received": 0, "latest_rate_date": str(latest)}
            first, last = window
            text = fetch_with_retries(fetch, series_url(codes, base_url, endpoint), _params(first, last), sleep)
            rates = parse(text, codes, first, last)
            added = append(cursor, rates, datetime.now(timezone.utc).replace(tzinfo=None))
        finally:
            cursor.close()
    latest = max([rate.rate_date for rate in rates] + ([latest] if latest else []), default=None)
    log.info("FX rates: asked the ECB for %s to %s (%s): %s rates received, %s added. The latest rate is of %s.",
             first, last, "+".join(codes), len(rates), added, latest)
    return {"state": "LOADED" if added else "UP_TO_DATE", "requested_from": str(first), "requested_to": str(last),
            "received": len(rates), "added": added, "latest_rate_date": str(latest)}


def write_seed(path: Path = FX_RATES_SEED, fetch: Callable[[str, dict], str] = fetch_csv,
               today: date | None = None, first: date = SEED_START) -> int:
    """Write dbt/seeds/fx_rates_ecb.csv: every rate from 2025-01-01 to today, as the ECB
    published it. Run once when the seed is added, and whenever a longer history is wanted in
    the repo. Returns the number of rates written."""
    today = today or datetime.now(timezone.utc).date()
    codes = currencies()
    try:
        base_url, endpoint = _endpoint(ECB_FX_CONN_ID)
    except connections.ConnectionNotFound:
        base_url, endpoint = BASE_URL, ENDPOINT
    rates = parse(fetch_with_retries(fetch, series_url(codes, base_url, endpoint), _params(first, today)),
                  codes, first, today)
    if not rates:
        raise PermanentError(f"The ECB API has no rate between {first} and {today}: the seed is not written.")
    with open(path, "w", newline="") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(["rate_date", "currency_code", "eur_rate"])
        writer.writerows((rate.rate_date.isoformat(), rate.currency_code, str(rate.eur_rate)) for rate in rates)
    log.info("FX rates: wrote %s rates of %s, %s to %s, to %s.", len(rates), "+".join(codes),
             rates[0].rate_date, rates[-1].rate_date, path)
    return len(rates)


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if argv == ["--write-seed"]:
        write_seed()
        return 0
    if argv:
        print("usage: python -m crm_pipeline.fx_rates [--write-seed]", file=sys.stderr)
        return 2
    print(load())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
