from __future__ import annotations

import csv
from contextlib import nullcontext
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
import requests

from crm_pipeline import config, connections, fx_rates, raw_tables
from crm_pipeline.errors import PermanentError
from crm_pipeline.fx_rates import Rate

# Historical data
SAMPLE = """KEY,FREQ,CURRENCY,CURRENCY_DENOM,EXR_TYPE,EXR_SUFFIX,TIME_PERIOD,OBS_VALUE
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-02,0.8358
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-03,0.83358
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-04,0.8346
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-05,0.83393
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-06,0.83415
EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-09,0.8343
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-02,1.1216
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-03,1.1179
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-04,1.1267
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-05,1.1224
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-06,1.1165
EXR.D.USD.EUR.SP00.A,D,USD,EUR,SP00,A,2026-11-09,1.1193
"""
CODES = ("GBP", "USD")
FIRST, LAST = date(2026, 11, 2), date(2026, 11, 9)
URL = "https://data-api.ecb.europa.eu/service/data/EXR/D.GBP+USD.EUR.SP00.A"


def answer(first: date, last: date) -> str:
    """What the API answers for a window: the sample's header and its rows of those days."""
    header, *rows = SAMPLE.splitlines()
    kept = [row for row in rows if first <= date.fromisoformat(row.split(",")[6]) <= last]
    return "\n".join([header, *kept]) + "\n" if kept else ""


class Warehouse:
    """RAW.REF.FX_RATES as a dict, behind the statements the load sends."""

    def __init__(self, held: dict | None = None):
        self.held = dict(held or {}) 
        self.statements: list[str] = []
        self.sessions = 0
        self._row = None

    def session(self):
        self.sessions += 1
        return nullcontext(self)

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.statements.append(sql)
        if sql.startswith("select max(RATE_DATE)"):
            self._row = (max((day for day, _ in self.held), default=None),)
        elif sql.startswith("select count(*)"):
            self._row = (len(self.held),)
        elif sql.startswith("insert"):
            assert "where not exists" in sql and "held.CURRENCY_CODE = v.currency_code" in sql     # the anti-join
            _, *values = params
            for day, currency, rate in zip(values[0::3], values[1::3], values[2::3], strict=True):
                self.held.setdefault((date.fromisoformat(day), currency), Decimal(rate))

    def fetchone(self):
        return self._row


@pytest.fixture
def connection(monkeypatch):
    """The ecb_fx_api connection, as .env.local.example has it."""
    conn = SimpleNamespace(extra={"base_url": "https://data-api.ecb.europa.eu", "endpoint": "/service/data/EXR"})
    monkeypatch.setattr(fx_rates.connections, "get_connection", lambda conn_id: conn)
    return conn


@pytest.fixture
def seed(tmp_path):
    """A seed that ends on 2026-11-05, so the load has the 6th and the 9th to ask for."""
    path = tmp_path / "fx_rates_ecb.csv"
    fx_rates.write_seed(path, fetch=lambda url, params: answer(FIRST, date(2026, 11, 5)), today=date(2026, 11, 5),
                        first=FIRST)
    return path


def load(warehouse, seed, today=LAST, fetch=None, sleeps=None):
    calls = []

    def default_fetch(url, params):
        calls.append((url, params))
        return answer(date.fromisoformat(params["startPeriod"]), date.fromisoformat(params["endPeriod"]))

    result = fx_rates.load(warehouse.session, fetch=fetch or default_fetch, today=today, seed=seed,
                           sleep=(sleeps if sleeps is not None else []).append)
    return result, calls



def test_the_api_s_answer_becomes_one_rate_per_currency_and_day_oldest_first():
    rates = fx_rates.parse(SAMPLE, CODES, FIRST, LAST)
    assert len(rates) == 12
    assert rates[0] == Rate(date(2026, 11, 2), "GBP", Decimal("0.8358"))
    assert rates[1] == Rate(date(2026, 11, 2), "USD", Decimal("1.1216"))
    assert rates[-1] == Rate(date(2026, 11, 9), "USD", Decimal("1.1193"))
    assert [rate.rate_date for rate in rates] == sorted(rate.rate_date for rate in rates)
    assert date(2026, 11, 7) not in {rate.rate_date for rate in rates}          # a Saturday has no rate
    # A rate is per euro: USD per GBP are the USD rate over the GBP rate
    by_key = {(rate.rate_date, rate.currency_code): rate.eur_rate for rate in rates}
    assert round(by_key[date(2026, 11, 6), "USD"] / by_key[date(2026, 11, 6), "GBP"], 4) == Decimal("1.3385")


def test_an_empty_answer_is_no_rate():
    assert fx_rates.parse("", CODES, date(2026, 11, 7), date(2026, 11, 8)) == []
    assert fx_rates.parse("\n", CODES, date(2026, 11, 7), date(2026, 11, 8)) == []


HEADER = SAMPLE.splitlines()[0]
ROW = "EXR.D.GBP.EUR.SP00.A,D,GBP,EUR,SP00,A,2026-11-02,0.8358"


@pytest.mark.parametrize("text, what", [
    ("<html><body>Service unavailable</body></html>", "expected header"),
    (HEADER.replace("OBS_VALUE", "OBS_VALUE,OBS_STATUS,OBS_CONF") + "\n" + ROW + ",A,F", "expected header"),
    (HEADER + "\n" + ROW.replace("GBP", "JPY"), "not a series that was asked for"),
    (HEADER + "\n" + ROW.replace(",D,", ",M,"), "not a series that was asked for"),
    (HEADER + "\n" + ROW.replace(",EUR,", ",USD,"), "not a series that was asked for"),
    (HEADER + "\n" + ROW.replace("2026-11-02", "2026-10-30"), "outside 2026-11-02 to 2026-11-09"),
    (HEADER + "\n" + ROW.replace("2026-11-02", "02/11/2026"), "no day or no number"),
    (HEADER + "\n" + ROW.replace("0.8358", "n/a"), "no day or no number"),
    (HEADER + "\n" + ROW.replace("0.8358", ""), "no day or no number"),
    (HEADER + "\n" + ROW.replace("0.8358", "0"), "not positive"),
    (HEADER + "\n" + ROW.replace("0.8358", "-0.8358"), "not positive"),
    (HEADER + "\n" + ROW.replace("0.8358", "NaN"), "not positive"),
    (HEADER + "\n" + ROW + "\n" + ROW, "two rates for GBP on 2026-11-02"),
    (HEADER + "\n" + ROW + ",extra", "9 fields, not 8"),
])
def test_an_answer_that_is_not_what_was_asked_for_is_refused_whole(text, what):
    with pytest.raises(PermanentError, match=what):
        fx_rates.parse(text, CODES, FIRST, LAST)


def test_the_currencies_asked_for_are_the_brands_and_usd():
    assert fx_rates.currencies() == CODES                   # WeCare and Greenz sell in GBP, Broomx in USD
    brands = [config.Brand("a", "A", "crm_a", True, "gbp"), config.Brand("b", "B", "crm_b", True, "EUR")]
    # USD is always asked for, to convert to, EUR is what the rates are measured in
    assert fx_rates.currencies(brands) == CODES
    assert fx_rates.series_url(CODES) == URL
    assert fx_rates.series_url(CODES, "https://example.org/", "/rates/") == "https://example.org/rates/D.GBP+USD.EUR.SP00.A"



@pytest.mark.parametrize("latest, today, window", [
    (None, date(2025, 1, 10), (date(2025, 1, 1), date(2025, 1, 10))),    
    (date(2026, 11, 6), date(2026, 11, 9), (date(2026, 11, 7), date(2026, 11, 9))),
    (date(2026, 11, 8), date(2026, 11, 9), (date(2026, 11, 9), date(2026, 11, 9))),
    (date(2026, 11, 9), date(2026, 11, 9), None),   
    (date(2026, 11, 10), date(2026, 11, 9), None),
])
def test_the_window_is_the_days_after_the_latest_rate(latest, today, window):
    assert fx_rates.request_window(latest, today) == window


def test_the_load_asks_for_the_days_after_the_seed_and_appends_them(connection, seed):
    warehouse = Warehouse()
    result, calls = load(warehouse, seed)
    assert calls == [(URL, {"startPeriod": "2026-11-06", "endPeriod": "2026-11-09", "format": "csvdata",
                            "detail": "dataonly"})]
    assert result == {"state": "LOADED", "requested_from": "2026-11-06", "requested_to": "2026-11-09",
                      "received": 4, "added": 4, "latest_rate_date": "2026-11-09"}
    assert warehouse.held == {
        (date(2026, 11, 6), "GBP"): Decimal("0.83415"), (date(2026, 11, 6), "USD"): Decimal("1.1165"),
        (date(2026, 11, 9), "GBP"): Decimal("0.8343"), (date(2026, 11, 9), "USD"): Decimal("1.1193"),
    }
    # The table is created before it is read, and the rows go in as one transaction
    kinds = [statement.split()[0] for statement in warehouse.statements]
    # Every table dbt does not wait for, when missing: this one, the CDC's and the clickstream's two
    assert kinds[:8] == ["create"] * 7 + ["select"]
    assert kinds[kinds.index("begin"):kinds.index("commit") + 1] == ["begin", "insert", "commit"]


def test_the_table_s_latest_rate_counts_when_it_is_later_than_the_seed_s(connection, seed):
    warehouse = Warehouse({(date(2026, 11, 6), "GBP"): Decimal("0.83415"), (date(2026, 11, 6), "USD"): Decimal("1.1165")})
    result, calls = load(warehouse, seed)
    assert calls[0][1]["startPeriod"] == "2026-11-07"
    assert (result["received"], result["added"], result["latest_rate_date"]) == (2, 2, "2026-11-09")


def test_a_second_run_the_same_day_asks_for_nothing_and_adds_nothing(connection, seed):
    warehouse = Warehouse()
    load(warehouse, seed)
    held = dict(warehouse.held)
    result, calls = load(warehouse, seed)
    assert calls == []
    assert result == {"state": "UP_TO_DATE", "added": 0, "received": 0, "latest_rate_date": "2026-11-09"}
    assert warehouse.held == held


def test_a_day_with_no_rate_yet_is_asked_for_again_and_adds_nothing(connection, seed):
    warehouse = Warehouse()
    load(warehouse, seed, today=date(2026, 11, 6))
    result, calls = load(warehouse, seed, today=date(2026, 11, 8))
    assert calls[0][1]["startPeriod"] == "2026-11-07" and calls[0][1]["endPeriod"] == "2026-11-08"
    assert (result["state"], result["received"], result["added"], result["latest_rate_date"]) == (
        "UP_TO_DATE", 0, 0, "2026-11-06")


def test_a_rate_that_arrives_twice_is_stored_once():
    warehouse = Warehouse()
    rates = fx_rates.parse(SAMPLE, CODES, FIRST, LAST)
    assert fx_rates.append(warehouse, rates, datetime(2026, 11, 9, 16, 30)) == 12
    assert fx_rates.append(warehouse, rates, datetime(2026, 11, 9, 17, 30)) == 0
    assert len(warehouse.held) == 12
    assert fx_rates.append(warehouse, [], datetime(2026, 11, 9, 17, 30)) == 0



def test_without_the_connection_the_load_skips_and_touches_nothing(monkeypatch, seed):
    def not_found(conn_id):
        raise connections.ConnectionNotFound(f"The conn_id `{conn_id}` isn't defined")

    monkeypatch.setattr(fx_rates.connections, "get_connection", not_found)
    warehouse = Warehouse()
    result, calls = load(warehouse, seed)
    assert result["state"] == "SKIPPED" and result["added"] == 0
    assert "No ecb_fx_api connection" in result["reason"]
    assert calls == [] and warehouse.sessions == 0 and warehouse.statements == []


def test_a_broken_secrets_backend_fails_instead_of_skipping(monkeypatch, seed):
    def broken(conn_id):
        raise RuntimeError("secrets backend is down")

    monkeypatch.setattr(fx_rates.connections, "get_connection", broken)
    with pytest.raises(RuntimeError, match="secrets backend is down"):
        load(Warehouse(), seed)


def test_three_tries_without_an_answer_fail_the_load_for_good(connection, seed):
    warehouse, sleeps, tries = Warehouse(), [], []

    def unavailable(url, params):
        tries.append(url)
        raise fx_rates.RatesUnavailable("The ECB API answered 503")

    with pytest.raises(PermanentError, match="did not answer in 3 tries: The ECB API answered 503"):
        load(warehouse, seed, fetch=unavailable, sleeps=sleeps)
    assert len(tries) == 3 and sleeps == [2, 8]         # a wait before the second and the third try
    assert warehouse.held == {}


def test_a_try_that_succeeds_after_a_failure_loads(connection, seed):
    warehouse, sleeps, tries = Warehouse(), [], []

    def flaky(url, params):
        tries.append(url)
        if len(tries) == 1:
            raise requests.ConnectionError("connection reset")
        return answer(date(2026, 11, 6), LAST)

    result, _ = load(warehouse, seed, fetch=flaky, sleeps=sleeps)
    assert (result["state"], result["added"]) == ("LOADED", 4)
    assert len(tries) == 2 and sleeps == [2]


def test_a_refused_request_fails_at_once(connection, seed):
    tries = []

    def refused(url, params):
        tries.append(url)
        raise PermanentError("The ECB API refused the request (404)")

    with pytest.raises(PermanentError, match="refused the request"):
        load(Warehouse(), seed, fetch=refused)
    assert len(tries) == 1                              # asking again gives the same answer


def test_an_answer_that_cannot_be_read_loads_nothing(connection, seed):
    warehouse = Warehouse()
    with pytest.raises(PermanentError, match="expected header"):
        load(warehouse, seed, fetch=lambda url, params: "<html>maintenance</html>")
    assert warehouse.held == {}
    assert "insert" not in {statement.split()[0] for statement in warehouse.statements}


@pytest.mark.parametrize("status, outcome", [(200, "text"), (404, PermanentError), (400, PermanentError),
                                             (429, fx_rates.RatesUnavailable), (503, fx_rates.RatesUnavailable)])
def test_one_request_says_who_asks_and_reads_the_status(monkeypatch, status, outcome):
    seen = {}

    def get(url, params, headers, timeout):
        seen.update(url=url, params=params, headers=headers, timeout=timeout)
        return SimpleNamespace(status_code=status, text="text")

    monkeypatch.setattr(fx_rates.requests, "get", get)
    if outcome == "text":
        assert fx_rates.fetch_csv(URL, {"startPeriod": "2026-11-06"}) == "text"
    else:
        with pytest.raises(outcome):
            fx_rates.fetch_csv(URL, {"startPeriod": "2026-11-06"})
    assert seen["url"] == URL and seen["timeout"] == 30
    # A generic name, and nobody's address
    assert seen["headers"]["User-Agent"] == "commerce-analytics-pipeline fx-rates loader"
    assert "@" not in seen["headers"]["User-Agent"]


# Seed
def test_the_seed_is_written_as_the_ecb_published_it(tmp_path):
    path = tmp_path / "seed.csv"
    assert fx_rates.write_seed(path, fetch=lambda url, params: SAMPLE, today=LAST, first=FIRST) == 12
    lines = path.read_text().splitlines()
    assert lines[0] == "rate_date,currency_code,eur_rate"
    assert lines[1:3] == ["2026-11-02,GBP,0.8358", "2026-11-02,USD,1.1216"]  
    assert lines[-1] == "2026-11-09,USD,1.1193"
    assert fx_rates.seed_latest(path) == LAST
    assert fx_rates.seed_latest(tmp_path / "missing.csv") is None


def test_no_seed_is_written_from_an_empty_answer(tmp_path):
    path = tmp_path / "seed.csv"
    with pytest.raises(PermanentError, match="the seed is not written"):
        fx_rates.write_seed(path, fetch=lambda url, params: "", today=LAST, first=FIRST)
    assert not path.exists()


def test_the_seed_in_the_repo_has_a_rate_for_every_order_from_the_first():
    with open(config.FX_RATES_SEED, newline="") as file:
        rows = list(csv.DictReader(file))
    keys = [(row["rate_date"], row["currency_code"]) for row in rows]
    assert keys == sorted(set(keys))                                    # in order, no day twice
    days = sorted({day for day, _ in keys})
    assert {currency for _, currency in keys} == set(fx_rates.currencies())
    assert len(keys) == len(days) * len(fx_rates.currencies())         # every currency on every day
    assert all(Decimal(row["eur_rate"]) > 0 for row in rows)
    assert date.fromisoformat(days[0]) == date(2025, 1, 2)
    assert fx_rates.seed_latest() == date.fromisoformat(days[-1])
    gaps = [(date.fromisoformat(b) - date.fromisoformat(a)).days for a, b in zip(days, days[1:])]
    assert max(gaps) == 5



SIDE_TABLES = ["RAW.REF.FX_RATES", "RAW.CRM.ORDERS_CDC", 'RAW.WEB."DAILY_FUNNEL"', 'RAW.WEB."EVENT_BATCHES"']


def test_the_side_tables_are_created_only_when_missing():
    warehouse = Warehouse()
    assert raw_tables.create_side_tables(warehouse) == SIDE_TABLES
    assert warehouse.statements[:2] == [
        "create schema if not exists RAW.REF",
        "create table if not exists RAW.REF.FX_RATES (RATE_DATE DATE, CURRENCY_CODE VARCHAR, EUR_RATE NUMBER(18, 6), "
        "_LOADED_AT TIMESTAMP_NTZ)",
    ]

    assert [statement.split(" (")[0] for statement in warehouse.statements[2:]] == [
        "create schema if not exists RAW.CRM", "create table if not exists RAW.CRM.ORDERS_CDC",
        "create schema if not exists RAW.WEB", 'create table if not exists RAW.WEB."DAILY_FUNNEL"',
        'create table if not exists RAW.WEB."EVENT_BATCHES"']
    assert all(" if not exists " in statement for statement in warehouse.statements)


def test_the_dbt_step_creates_them_in_a_session_of_its_own():
    warehouse = Warehouse()
    assert raw_tables.ensure_side_tables(warehouse.session) == SIDE_TABLES
    assert warehouse.sessions == 1
    assert [statement.split()[0] for statement in warehouse.statements] == ["create"] * 7


def test_the_command_line_knows_two_things(capsys):
    assert fx_rates.main(["--write-everything"]) == 2
    assert "usage: python -m crm_pipeline.fx_rates [--write-seed]" in capsys.readouterr().err
