"""CDC of the orders table."""
from __future__ import annotations

import json
import logging
import re
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path

import pytest
import yaml

from crm_pipeline import bronze_loader, cdc, connections, raw_tables
from crm_pipeline.cdc import Change
from crm_pipeline.config import SOURCE_TABLES, Brand, SourceTable
from crm_pipeline.errors import PermanentError
from crm_pipeline.source_postgres import SourceColumn

REPO = Path(__file__).resolve().parents[1]
LOCAL_SH = (REPO / "scripts" / "local.sh").read_text()


TYPES = {
    "id": "integer", "number": "character varying(32)", "customer_id": "integer", "created_from_order_id": "integer",
    "state": "character varying(32)", "payment_state": "character varying(32)", "shipping_state": "character varying(32)",
    "checkout_completed_at": "timestamp without time zone", "items_total": "integer", "adjustments_total": "integer",
    "total": "integer", "currency_code": "character(3)", "is_subscription": "boolean",
    "is_subscription_canceled": "boolean", "subscription_canceled_at": "timestamp without time zone",
    "subscription_paused": "boolean", "is_created_by_admin": "boolean", "is_sales_funnel": "boolean",
    "acquisition_domain": "character varying(255)", "utm_source": "character varying(255)",
    "utm_medium": "character varying(255)", "utm_campaign": "character varying(255)",
    "utm_content": "character varying(255)", "utm_term": "character varying(255)", "utm_id": "character varying(255)",
    "created_at": "timestamp without time zone", "updated_at": "timestamp without time zone",
}
ORDER = {
    "id": 900001, "number": "P-900001", "customer_id": 1, "created_from_order_id": None, "state": "cart",
    "payment_state": None, "shipping_state": None, "checkout_completed_at": None, "items_total": 1000,
    "adjustments_total": 0, "total": 1000, "currency_code": "USD", "is_subscription": False,
    "is_subscription_canceled": False, "subscription_canceled_at": None, "subscription_paused": False,
    "is_created_by_admin": False, "is_sales_funnel": False, "acquisition_domain": None,
    "utm_source": 'a "quoted" söurce', "utm_medium": None, "utm_campaign": None, "utm_content": None, "utm_term": None,
    "utm_id": None, "created_at": "2026-11-10 12:00:00.123456", "updated_at": "2026-11-10 12:00:00.123456",
}
COMMITTED = "2026-11-11 01:01:32.761424+00"


def columns(row: dict) -> list[dict]:
    return [{"name": name, "type": TYPES[name], "value": value} for name, value in row.items()]


def message(action: str, row: dict | None = None, table: str = "orders", **more) -> str:
    body = {"action": action, "timestamp": COMMITTED}
    if action in ("I", "U"):
        body.update(schema="public", table=table, columns=columns(row))
    if action in ("U", "D"):
        body.update(schema="public", table=table, identity=[{"name": "id", "type": "integer", "value": row["id"]}])
    return json.dumps({**body, **more})


PAID = {**ORDER, "state": "new", "payment_state": "paid", "checkout_completed_at": "2026-11-10 12:05:00",
        "updated_at": "2026-11-10 12:05:00"}
SUBSCRIBED = {**PAID, "is_subscription": True, "updated_at": "2026-11-10 12:06:00"}
SECOND = {**ORDER, "id": 900002, "number": "P-900002", "customer_id": None, "utm_source": None}
# (lsn, xid, data), as the function returns them. Transaction 757 touched another table only: its
# begin and its commit come all the same. A change row has the place of its own record, a commit
# row the end of the commit, where the next transaction begins
ROWS = [
    ("0/1BBBEE8", "757", message("B")),
    ("0/1BBC0D8", "757", message("C")),
    ("0/1BBC0D8", "758", message("B")),
    ("0/1BBC0D8", "758", message("I", ORDER)),
    ("0/1BBC320", "758", message("C")),
    ("0/1BBC320", "759", message("B")),
    ("0/1BBC320", "759", message("U", PAID)),
    ("0/1BBC400", "759", message("C")),
    ("0/1BBC4A0", "761", message("B")),
    ("0/1BBC4A0", "761", message("U", SUBSCRIBED)),
    ("0/1BBC550", "761", message("I", SECOND)),
    ("0/1BBC790", "761", message("C")),
    ("0/1BBC790", "762", message("B")),
    ("0/1BBC790", "762", message("D", ORDER)),
    ("0/1BBC808", "762", message("C")),
]
ORDERS = next(table for table in SOURCE_TABLES if table.name == "orders")
CUSTOMERS = next(table for table in SOURCE_TABLES if table.name == "customers")


class Slot:
    """A replication slot behind the statements the capture sends to PostgreSQL: the rows it has
    not confirmed yet, and where it is."""

    def __init__(self, rows=ROWS, confirmed="0/1BBBEA0", flushed="0/1C00000", wal_status="reserved", reason=None,
                 plugin="wal2json", database="crm_broomx", exists=True, fail_advance=0):
        self.rows, self.confirmed, self.flushed = list(rows), confirmed, flushed
        self.wal_status, self.reason, self.plugin, self.database, self.exists = wal_status, reason, plugin, database, exists
        self.fail_advance = fail_advance        # how many advances fail first, as a crash before them would
        self.calls: list[tuple] = []
        self.autocommit = False
        self._answer = None

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def pending(self) -> list[tuple]:
        return [row for row in self.rows if cdc.lsn_number(row[0]) > cdc.lsn_number(self.confirmed)
                or (json.loads(row[2])["action"] != "C" and cdc.lsn_number(row[0]) == cdc.lsn_number(self.confirmed))]

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        if sql.startswith("set "):
            self.calls.append(("set", sql))
        elif "pg_logical_slot_peek_changes" in sql:
            slot, limit, tables = params
            self.calls.append(("peek", slot, limit, tables))
            assert "'format-version', '2'" in sql and "'add-tables', %s" in sql
            answer = []
            for row in self.pending():      # whole transactions: it stops at the first commit past the limit
                answer.append(row)
                if json.loads(row[2])["action"] == "C" and len(answer) >= limit:
                    break
            self._answer = answer
        elif "pg_replication_slot_advance" in sql:
            slot, lsn = params
            self.calls.append(("advance", slot, lsn))
            if self.fail_advance:
                self.fail_advance -= 1
                raise ConnectionError("the connection to the source was lost")
            assert cdc.lsn_number(lsn) >= cdc.lsn_number(self.confirmed), "PostgreSQL refuses to move a slot backwards"
            self.confirmed = lsn
            self._answer = [(lsn,)]
        elif sql.startswith("select pg_current_wal_flush_lsn()"):
            self._answer = [(self.flushed,)]
        elif sql.startswith("select s.plugin"):
            self.calls.append(("check", params[0]))
            self._answer = ([(self.plugin, self.database, self.wal_status, self.reason, self.confirmed)]
                            if self.exists else [])
        elif sql.startswith("select confirmed_flush_lsn"):
            self._answer = [(self.confirmed, cdc.lsn_number(self.flushed) - cdc.lsn_number(self.confirmed))]
        else:
            raise AssertionError(f"a statement the capture is not expected to send: {sql}")

    def fetchone(self):
        return self._answer[0] if self._answer else None

    def fetchall(self):
        return self._answer


class Warehouse:
    """RAW.CRM.ORDERS_CDC as a dict, behind the statements the capture sends to the warehouse."""

    def __init__(self):
        self.held: dict[tuple, dict] = {}       # (brand, lsn as a number, id) -> the row
        self.statements: list[str] = []
        self.sessions = 0
        self._answer = None
        self._open: dict | None = None          # the rows of a transaction that has not committed

    def session(self):
        self.sessions += 1
        return nullcontext(self)

    def cursor(self):
        return self

    def close(self):
        pass

    def execute(self, sql, params=None):
        sql = " ".join(sql.split())
        self.statements.append(sql.split(" (")[0] if sql.startswith(("insert", "create")) else sql)
        if sql == "begin":
            self._open = dict(self.held)
        elif sql == "commit":
            self.held, self._open = self._open, None
        elif sql == "rollback":
            self._open = None
        elif sql.startswith("insert into RAW.CRM.ORDERS_CDC"):
            # The anti-join: brand, the place in the log and the row's id
            assert "where not exists (select 1 from RAW.CRM.ORDERS_CDC as held where held._BRAND = %s" in sql
            assert 'held._LSN_NUMBER = v.lsn_number::number(38, 0) and held."ID" = v.c0::number(38, 0)' in sql
            names = [*raw_tables.ORDERS_CDC_COLUMNS, "_OP", "_LSN", "_LSN_NUMBER", "_XID", "_COMMITTED_AT"]
            brand, loaded_at, *values = params[:-2]
            assert params[-2] == brand and len(values) % len(names) == 0
            added = 0
            for start in range(0, len(values), len(names)):
                row = dict(zip(names, values[start:start + len(names)]), _BRAND=brand, _LOADED_AT=loaded_at)
                key = (brand, row["_LSN_NUMBER"], row["ID"])
                if key not in self._open:
                    self._open[key] = row
                    added += 1
            self._answer = (added,)

    def fetchone(self):
        return self._answer

    def ops(self) -> list[str]:
        return [row["_OP"] for _, row in sorted(self.held.items(), key=lambda item: item[0][1])]


def capture(slot: Slot, warehouse: Warehouse, **kwargs) -> dict:
    return cdc.capture("broomx", "crm_broomx", warehouse.session, connect=lambda database, conn_id: slot, **kwargs)


def test_an_insert_carries_the_whole_row_with_json_s_own_numbers_booleans_and_nulls():
    action, change = cdc.decode("0/1BBC0D8", "758", message("I", ORDER))
    assert action == "I"
    assert (change.op, change.lsn, change.xid) == ("I", "0/1BBC0D8", 758)
    assert change.values["ID"] == 900001 and change.values["TOTAL"] == 1000         # numbers
    assert change.values["IS_SUBSCRIPTION"] is False                                 # a boolean
    assert change.values["PAYMENT_STATE"] is None and "PAYMENT_STATE" in change.values     # a NULL is sent, as null
    assert change.values["UTM_SOURCE"] == 'a "quoted" söurce'
    assert change.values["UPDATED_AT"] == "2026-11-10 12:00:00.123456"               # a timestamp is text
    assert set(change.values) == set(raw_tables.ORDERS_CDC_COLUMNS)                  # every column of the table
    # The commit time, with its zone, becomes UTC without one: a TIMESTAMP_NTZ
    assert change.committed_at == datetime(2026, 11, 11, 1, 1, 32, 761424)


def test_an_update_carries_the_row_as_it_is_after_the_change():
    _, change = cdc.decode("0/1BBC320", "759", message("U", PAID))
    assert change.op == "U"
    assert (change.values["STATE"], change.values["PAYMENT_STATE"]) == ("new", "paid")
    assert change.values["CHECKOUT_COMPLETED_AT"] == "2026-11-10 12:05:00"           # no fraction when it is zero


def test_a_delete_carries_the_key_only():
    _, change = cdc.decode("0/1BBC790", "762", message("D", ORDER))
    assert change.op == "D"
    assert change.values == {"ID": 900001}


def test_a_begin_and_a_commit_are_no_change():
    assert cdc.decode("0/1BBBEE8", "757", message("B")) == ("B", None)
    assert cdc.decode("0/1BBC0D8", "757", message("C")) == ("C", None)


def test_a_commit_time_in_another_zone_is_the_same_moment_in_utc():
    assert cdc._utc("2026-11-10 20:01:32.5-05") == datetime(2026, 11, 11, 1, 1, 32, 500000)
    assert cdc._utc(None) is None


def test_a_place_in_the_log_orders_as_a_number_where_its_text_does_not():
    assert cdc.lsn_number("0/1BBC320") == 0x1BBC320
    assert cdc.lsn_number("1/0") == 1 << 32
    places = ["0/9FFFFFF", "0/A000000", "0/10000000", "1/0"]
    assert sorted(places, key=cdc.lsn_number) == places
    assert sorted(places) != places             # as text, 0/10000000 comes before 0/9FFFFFF


def test_an_action_the_capture_does_not_know_stops_it():
    with pytest.raises(PermanentError, match="an action this capture does not know at 0/1BBC0D8: 'T'"):
        cdc.decode("0/1BBC0D8", "758", message("T", table="orders"))


# PII
CUSTOMER = {"id": 7, "email": "someone@example.com", "country_code": "US"}
CUSTOMER_MESSAGE = json.dumps({"action": "I", "schema": "public", "table": "customers", "columns": [
    {"name": "id", "type": "integer", "value": 7},
    {"name": "email", "type": "character varying(255)", "value": "someone@example.com"},
    {"name": "country_code", "type": "character(2)", "value": "US"}]})


def test_only_the_orders_table_is_asked_for():
    # Logical decoding ignores table privileges: a slot streams every table of its database,
    # customers.email included. The capture names the one table it wants in every read
    assert [table.name for table in cdc.CDC_TABLES] == ["orders"]
    assert cdc.add_tables() == "public.orders"
    slot, warehouse = Slot(), Warehouse()
    capture(slot, warehouse)
    peeks = [call for call in slot.calls if call[0] == "peek"]
    assert peeks and all(call[3] == "public.orders" for call in peeks)


def test_a_change_of_a_table_that_was_not_asked_for_stops_the_capture_and_shows_no_value():
    with pytest.raises(PermanentError) as caught:
        cdc.decode("0/1BBBEE8", "757", CUSTOMER_MESSAGE)
    text = str(caught.value)
    assert "public.customers" in text and "was not asked for" in text
    assert "someone@example.com" not in text and "email" not in text.lower()


def test_a_configuration_that_includes_customers_never_yields_the_email(monkeypatch):
    # The second guard, the loader's own: SourceTable.keeps drops what bronze must never hold,
    # before anything else looks at the row
    monkeypatch.setattr(cdc, "ORDERS_CDC_COLUMNS", {"ID": "NUMBER(38, 0)", "COUNTRY_CODE": "VARCHAR"})
    _, change = cdc.decode("0/1BBBEE8", "757", CUSTOMER_MESSAGE, tables=(CUSTOMERS, ORDERS))
    assert change.values == {"ID": 7, "COUNTRY_CODE": "US"}
    assert "someone@example.com" not in repr(change)
    # And a table configured with no exclusion at all would still not get past the table's columns
    monkeypatch.undo()
    with pytest.raises(PermanentError) as caught:
        cdc.decode("0/1BBBEE8", "757", CUSTOMER_MESSAGE, tables=(SourceTable("customers"), ORDERS))
    assert "COUNTRY_CODE, EMAIL" in str(caught.value) and "someone@example.com" not in str(caught.value)


def test_nothing_of_a_message_reaches_the_log(caplog):
    slot, warehouse = Slot(), Warehouse()
    with caplog.at_level(logging.DEBUG, logger="crm_pipeline"):
        capture(slot, warehouse)
    assert "CDC orders: broomx captured 5 changes (2 inserts, 2 updates, 1 deletes) in 4 transactions" in caplog.text
    for value in ("P-900001", "söurce", "900001"):
        assert value not in caplog.text


def test_the_reading_role_may_read_the_log_and_nothing_else():
    setup = (REPO / "local" / "source_cdc.sql").read_text()
    statements = [line for line in setup.splitlines() if not line.lstrip().startswith("--")]
    assert "CREATE ROLE crm_cdc_reader LOGIN REPLICATION PASSWORD" in "\n".join(statements)
    for more in ("GRANT", "SUPERUSER", "CREATEDB", "CREATEROLE", "BYPASSRLS"):
        assert more not in "\n".join(statements)
    for seed in ("zz_users.sql", "fake_crm.sql"):
        assert "crm_cdc_reader" not in (REPO / "local" / "source_db" / seed).read_text()
    connection = next(line for line in (REPO / ".env.local.example").read_text().splitlines()
                      if line.startswith("AIRFLOW_CONN_CRM_SOURCE_CDC="))
    assert connection.split("=", 1)[1].strip("'") == "postgresql://crm_cdc_reader:crm_cdc_reader@127.0.0.1:5433"


def test_a_capture_appends_every_change_and_moves_the_slot_to_the_last_commit():
    slot, warehouse = Slot(), Warehouse()
    result = capture(slot, warehouse)
    assert result == {
        "state": "CAPTURED", "brand": "broomx", "slot": "cdc_crm_broomx", "changes": 5, "inserts": 2, "updates": 2,
        "deletes": 1, "transactions": 4, "added": 5, "first_lsn": "0/1BBC0D8", "last_lsn": "0/1BBC808",
        "slot_lsn": "0/1C00000", "lag_bytes": 0,
    }
    assert warehouse.ops() == ["I", "U", "U", "I", "D"]             # in the order of the log
    deleted = warehouse.held["broomx", cdc.lsn_number("0/1BBC790"), 900001]
    assert deleted["_OP"] == "D" and deleted["STATE"] is None and deleted["UPDATED_AT"] is None
    inserted = warehouse.held["broomx", cdc.lsn_number("0/1BBC0D8"), 900001]
    assert (inserted["_LSN"], inserted["_XID"], inserted["_COMMITTED_AT"]) == (
        "0/1BBC0D8", 758, "2026-11-11 01:01:32.761424")
    assert inserted["IS_SUBSCRIPTION"] is False and inserted["UPDATED_AT"] == "2026-11-10 12:00:00.123456"
    assert slot.autocommit is True          # a slot is held only while a statement reads it
    assert [call[1] for call in slot.calls if call[0] == "set"] == ["set time zone 'UTC'", "set datestyle = 'ISO, YMD'"]


def test_the_write_is_committed_before_the_slot_moves():
    slot, warehouse = Slot(), Warehouse()
    order = []
    advance, execute = slot.execute, warehouse.execute

    def source(sql, params=()):
        if "pg_replication_slot_advance" in sql or "pg_logical_slot_peek_changes" in sql:
            order.append("advance" if "advance" in sql else "peek")
        return advance(sql, params)

    def target(sql, params=None):
        if sql in ("begin", "commit") or sql.startswith("insert"):
            order.append(sql.split()[0])
        return execute(sql, params)

    slot.execute, warehouse.execute = source, target
    capture(slot, warehouse)
    # The last read is empty, and the slot then moves to the flush position read before it
    assert order == ["peek", "begin", "insert", "commit", "advance", "peek", "advance"]
    assert [call[2] for call in slot.calls if call[0] == "advance"] == ["0/1BBC808", "0/1C00000"]


def test_the_slot_only_ever_moves_to_a_commit_or_past_an_empty_read():
    slot, warehouse = Slot(), Warehouse()
    capture(slot, warehouse, batch_changes=2)
    commits = {lsn for lsn, _, data in ROWS if json.loads(data)["action"] == "C"}
    advances = [call[2] for call in slot.calls if call[0] == "advance"]
    assert set(advances[:-1]) <= commits and advances[-1] == slot.flushed
    # Moved into the middle of a transaction, the next read would return the whole of it again
    assert not set(advances) & {"0/1BBC4A0", "0/1BBC550"}


def test_a_long_backlog_comes_in_batches_of_whole_transactions():
    slot, warehouse = Slot(), Warehouse()
    result = capture(slot, warehouse, batch_changes=2)
    peeks = [call for call in slot.calls if call[0] == "peek"]
    assert len(peeks) > 2 and all(call[2] == 2 for call in peeks)
    assert result["changes"] == 5 and warehouse.ops() == ["I", "U", "U", "I", "D"]
    # The two changes of transaction 761 arrived, and were written, together
    # One write per read that held a change: the first read is a transaction of another table,
    # the last one is empty
    statements = [statement for statement in warehouse.statements if statement in ("begin", "commit")]
    assert statements.count("begin") == statements.count("commit") == len(peeks) - 2 == 4


def test_a_crash_between_the_write_and_the_advance_writes_nothing_twice():
    slot, warehouse = Slot(fail_advance=1), Warehouse()
    with pytest.raises(ConnectionError):
        capture(slot, warehouse)
    assert len(warehouse.held) == 5 and slot.confirmed == "0/1BBBEA0"       # written, and the slot did not move
    result = capture(slot, warehouse)                                       # the next run reads the same changes
    assert (result["changes"], result["added"]) == (5, 0)                   # and the anti-join drops them all
    assert len(warehouse.held) == 5 and warehouse.ops() == ["I", "U", "U", "I", "D"]
    assert slot.confirmed == "0/1C00000"


def test_a_second_run_with_no_new_activity_captures_nothing():
    slot, warehouse = Slot(), Warehouse()
    capture(slot, warehouse)
    statements = len(warehouse.statements)
    result = capture(slot, warehouse)
    assert (result["state"], result["changes"], result["added"], result["lag_bytes"]) == ("UP_TO_DATE", 0, 0, 0)
    assert result["first_lsn"] is None and result["slot_lsn"] == "0/1C00000"
    assert not [statement for statement in warehouse.statements[statements:] if statement.startswith(("insert", "begin"))]
    assert [call[0] for call in slot.calls][-2:] == ["check", "peek"]       # at the flush position already: no advance


def test_a_quiet_database_s_slot_follows_the_server_s_log():
    # Every database writes to one log. With nothing of its own to read, the slot would stay
    # where it is, keep the log files, and be invalidated at max_slot_wal_keep_size
    slot, warehouse = Slot(rows=[], confirmed="0/1BBC808", flushed="0/2C8C090"), Warehouse()
    result = capture(slot, warehouse)
    assert result["state"] == "UP_TO_DATE"
    assert [call for call in slot.calls if call[0] == "advance"] == [("advance", "cdc_crm_broomx", "0/2C8C090")]
    assert (result["slot_lsn"], result["lag_bytes"]) == ("0/2C8C090", 0)


def test_a_read_without_a_commit_does_not_move_the_slot():
    with pytest.raises(RuntimeError, match="held no commit"):
        cdc.read(ROWS[2:4])
    # Rows after the last commit of a read are left for the next one
    changes, commit = cdc.read(ROWS[:7])
    assert [change.op for change in changes] == ["I"] and commit == "0/1BBC320"


def test_without_its_connection_the_capture_skips_and_opens_nothing(caplog):
    warehouse = Warehouse()

    def no_connection(database, conn_id):
        raise connections.ConnectionNotFound(f"The conn_id `{conn_id}` isn't defined")

    with caplog.at_level(logging.INFO, logger="crm_pipeline.cdc"):
        result = cdc.capture("broomx", "crm_broomx", warehouse.session, connect=no_connection)
    assert result["state"] == "SKIPPED" and result["changes"] == 0
    assert "No crm_source_cdc connection" in result["reason"]
    assert "CDC orders: skipped. No crm_source_cdc connection" in caplog.text
    assert warehouse.sessions == 0 and warehouse.statements == []


def test_a_connection_that_is_there_and_fails_is_not_a_skip():
    def refused(database, conn_id):
        raise ConnectionRefusedError("connection refused")

    with pytest.raises(ConnectionRefusedError):
        cdc.capture("broomx", "crm_broomx", Warehouse().session, connect=refused)


def test_an_invalidated_slot_stops_the_capture_and_says_how_to_create_it_again():
    # What PostgreSQL shows once max_slot_wal_keep_size removed the log a slot had not read
    slot, warehouse = Slot(wal_status="lost", reason="wal_removed", confirmed="0/1799218"), Warehouse()
    with pytest.raises(PermanentError) as caught:
        capture(slot, warehouse)
    text = str(caught.value)
    assert "cdc_crm_broomx was invalidated (wal_removed)" in text and "since 0/1799218" in text
    assert "select pg_drop_replication_slot('cdc_crm_broomx'); " in text
    assert "select pg_create_logical_replication_slot('cdc_crm_broomx', 'wal2json')" in text
    assert "scripts/local.sh source-db up" in text
    assert not [call for call in slot.calls if call[0] in ("peek", "advance")]
    assert warehouse.held == {} and "begin" not in warehouse.statements


def test_a_missing_slot_and_a_slot_of_another_kind_stop_it_too():
    with pytest.raises(PermanentError, match="cdc_crm_broomx does not exist.*pg_create_logical_replication_slot"):
        capture(Slot(exists=False), Warehouse())
    with pytest.raises(PermanentError, match="is a pgoutput slot of crm_broomx, not a wal2json slot"):
        capture(Slot(plugin="pgoutput"), Warehouse())
    with pytest.raises(PermanentError, match="is a wal2json slot of crm_wecare, not a wal2json slot of crm_broomx"):
        capture(Slot(database="crm_wecare"), Warehouse())


def test_a_column_the_table_lacks_stops_the_capture_before_anything_is_written():
    grown = json.loads(message("I", ORDER))
    grown["columns"].append({"name": "gift_note", "type": "text", "value": "for you"})
    rows = [("0/1BBC0D8", "758", message("B")), ("0/1BBC0D8", "758", json.dumps(grown)), ("0/1BBC320", "758", message("C"))]
    slot, warehouse = Slot(rows=rows), Warehouse()
    with pytest.raises(PermanentError) as caught:
        capture(slot, warehouse)
    assert "columns RAW.CRM.ORDERS_CDC does not: GIFT_NOTE" in str(caught.value)
    assert "ORDERS_CDC_COLUMNS in crm_pipeline/raw_tables.py" in str(caught.value) and "for you" not in str(caught.value)
    assert warehouse.held == {} and slot.confirmed == "0/1BBBEA0"


BRANDS = [Brand("broomx", "Broomx", "crm_broomx", True), Brand("wecare", "WeCare", "crm_wecare", True),
          Brand("greenz", "Greenz", "crm_greenz", True)]


def test_every_brand_is_captured_and_the_totals_add_up():
    calls = []

    def one(brand, database, open_session):
        calls.append((brand, database, open_session))
        return {"state": "CAPTURED" if brand == "wecare" else "UP_TO_DATE", "changes": 3 if brand == "wecare" else 0,
                "inserts": 2 if brand == "wecare" else 0, "updates": 1 if brand == "wecare" else 0, "deletes": 0,
                "added": 3 if brand == "wecare" else 0, "lag_bytes": {"broomx": 0, "wecare": 56, "greenz": 0}[brand]}

    result = cdc.capture_all(BRANDS, "the session", capture_brand=one)
    assert calls == [("broomx", "crm_broomx", "the session"), ("wecare", "crm_wecare", "the session"),
                     ("greenz", "crm_greenz", "the session")]
    assert {key: result[key] for key in ("state", "changes", "inserts", "updates", "deletes", "added", "lag_bytes")} == {
        "state": "CAPTURED", "changes": 3, "inserts": 2, "updates": 1, "deletes": 0, "added": 3, "lag_bytes": 56}
    assert set(result["brands"]) == {"broomx", "wecare", "greenz"}


def test_all_brands_skipped_is_skipped_and_none_new_is_up_to_date():
    skipped = cdc.capture_all(BRANDS, None, capture_brand=lambda *args: {"state": "SKIPPED", "changes": 0, "added": 0})
    assert (skipped["state"], skipped["changes"], skipped["lag_bytes"]) == ("SKIPPED", 0, 0)
    quiet = cdc.capture_all(BRANDS, None, capture_brand=lambda *args: {"state": "UP_TO_DATE", "changes": 0, "added": 0})
    assert quiet["state"] == "UP_TO_DATE"


def test_a_brand_that_fails_does_not_stop_the_others_and_the_step_then_fails():
    tried = []

    def one(brand, database, open_session):
        tried.append(brand)
        if brand == "broomx":
            raise ConnectionError("the source did not answer")
        if brand == "wecare":
            raise PermanentError("The replication slot cdc_crm_wecare was invalidated")
        return {"state": "UP_TO_DATE", "changes": 0, "added": 0}

    # A slot that is gone stays gone: the permanent failure is the one raised, so nothing retries
    with pytest.raises(PermanentError, match="cdc_crm_wecare was invalidated"):
        cdc.capture_all(BRANDS, None, capture_brand=one)
    assert tried == ["broomx", "wecare", "greenz"]

    def flaky(brand, database, open_session):
        if brand == "greenz":
            raise ConnectionError("the source did not answer")
        return {"state": "UP_TO_DATE", "changes": 0, "added": 0}

    with pytest.raises(ConnectionError):
        cdc.capture_all(BRANDS, None, capture_brand=flaky)


def test_the_command_line_takes_no_argument(capsys):
    assert cdc.main(["--everything"]) == 2
    assert "usage: python -m crm_pipeline.cdc" in capsys.readouterr().err


def source_orders_columns() -> list[SourceColumn]:
    """The columns of the orders table in fake_crm.sql, as information_schema names their types."""
    text = (REPO / "local" / "source_db" / "fake_crm.sql").read_text()
    body = re.search(r"^CREATE TABLE orders \((.*?)^\);", text, re.S | re.M).group(1)
    kinds = {"INTEGER": "integer", "VARCHAR": "character varying", "CHAR": "character", "BOOLEAN": "boolean",
             "TIMESTAMP": "timestamp without time zone"}
    found = []
    for line in body.splitlines():
        match = re.match(r"\s+([a-z_]+) ([A-Z]+)", line)
        if match:
            found.append(SourceColumn(match.group(1), kinds[match.group(2)], None, None))
    return found


def test_the_table_has_the_columns_of_the_source_s_orders_with_the_loader_s_types():
    # The list is fixed in raw_tables.py, because the dbt step creates the table with no
    # connection to the source. This holds it against the source's own definition
    source = source_orders_columns()
    assert len(source) == 27
    assert {column.name.upper(): bronze_loader.snowflake_type(column) for column in source} == raw_tables.ORDERS_CDC_COLUMNS
    assert list(raw_tables.ORDERS_CDC_COLUMNS) == [column.name.upper() for column in source]      # and their order
    assert all(ORDERS.keeps(column.name) for column in source)
    assert list(raw_tables.ORDERS_CDC_META) == ["_OP", "_LSN", "_LSN_NUMBER", "_XID", "_COMMITTED_AT", "_BRAND", "_LOADED_AT"]


def test_the_dbt_step_creates_the_table_when_it_is_missing():
    warehouse = Warehouse()
    assert raw_tables.ORDERS_CDC_TABLE in raw_tables.create_side_tables(warehouse)
    assert "create schema if not exists RAW.CRM" in warehouse.statements
    assert "create table if not exists RAW.CRM.ORDERS_CDC" in warehouse.statements
    ddl = raw_tables.SIDE_TABLES["RAW.CRM"][raw_tables.ORDERS_CDC_TABLE]
    # Quoted, as the loader's columns are: NUMBER is a column of orders and a type
    assert ddl.startswith('"ID" NUMBER(38, 0), "NUMBER" VARCHAR, ')
    assert ddl.endswith("_COMMITTED_AT TIMESTAMP_NTZ, _BRAND VARCHAR, _LOADED_AT TIMESTAMP_NTZ")


def test_the_capture_never_writes_the_loader_s_tables():
    slot, warehouse = Slot(), Warehouse()
    capture(slot, warehouse)
    written = {statement.split()[2] for statement in warehouse.statements if statement.startswith("insert into")}
    assert written == {"RAW.CRM.ORDERS_CDC"}
    source = (REPO / "dags" / "crm_pipeline" / "cdc.py").read_text()
    # Its code names one table, and has no statement that changes a row that is there
    for forbidden in ("merge into", "delete from", "ORDERS_CHANGES", "changes_table", "qualified("):
        assert forbidden not in source.split('"""', 2)[2], forbidden


def test_one_insert_writes_a_chunk_and_names_the_columns_by_their_place():
    changes = [Change("I", "0/1BBC0D8", 758, datetime(2026, 11, 11, 1, 1, 32), {"ID": 1, "STATE": "cart"}),
               Change("D", "0/1BBC790", 762, None, {"ID": 1})]
    sql, params = cdc._insert("broomx", datetime(2026, 11, 11, 2, 0), changes, cdc.lsn_number("0/1BBC0D8"))
    assert sql.count("%s") == len(params) == 2 + 2 * (27 + 5) + 2
    assert sql.startswith('insert into RAW.CRM.ORDERS_CDC ("ID", "NUMBER", ')
    assert 'v.c0::NUMBER(38, 0), v.c1::VARCHAR' in sql and "v.committed_at::timestamp_ntz, %s, %s::timestamp_ntz" in sql
    assert params[:2] == ["broomx", "2026-11-11 02:00:00"] and params[-2:] == ["broomx", cdc.lsn_number("0/1BBC0D8")]
    first = params[2:2 + 32]
    assert first[0] == 1 and first[4] == "cart" and first[1] is None                    # a column the change lacks is NULL
    assert first[27:] == ["I", "0/1BBC0D8", cdc.lsn_number("0/1BBC0D8"), 758, "2026-11-11 01:01:32"]
    assert params[2 + 32:2 + 64][27:] == ["D", "0/1BBC790", cdc.lsn_number("0/1BBC790"), 762, None]


def function(name: str) -> str:
    body = LOCAL_SH[LOCAL_SH.index(f"\n{name}() {{"):]
    return body[:body.index("\n}\n")]


SETTINGS = ["wal_level=logical", "max_slot_wal_keep_size=1GB", "output_plugin_libraries=pgoutput,test_decoding,wal2json"]


def test_the_server_is_started_for_logical_decoding_with_wal2json_allowed():
    block = LOCAL_SH[LOCAL_SH.index("SOURCE_SERVER_SETTINGS=("):]
    block = block[:block.index(")")]
    assert [setting.strip('"') for setting in re.findall(r"-c (\S+)", block)] == SETTINGS
    assert '"$SOURCE_IMAGE" "${SOURCE_SERVER_SETTINGS[@]}"' in function("source_db_up")
    # compose.yaml describes the same container, and is kept in step
    service = yaml.safe_load((REPO / "local" / "compose.yaml").read_text())["services"]["crm-source"]
    assert [value for value in service["command"] if value != "-c"] == SETTINGS
    assert service["build"] == {"context": "./source_db", "dockerfile": "Containerfile"}


def test_the_image_is_pinned_and_a_change_to_it_replaces_the_container():
    containerfile = (REPO / "local" / "source_db" / "Containerfile").read_text()
    instructions = [line for line in containerfile.splitlines() if line and not line.startswith("#")]
    assert re.fullmatch(r"FROM docker\.io/library/postgres:18@sha256:[0-9a-f]{64}", instructions[0])
    assert re.search(r"postgresql-18-wal2json=\d+\.\d+-\d+\.pgdg\d+\+\d+ ", containerfile)       # an exact version
    # The tag is the file's hash: another Containerfile is another image, which source_db_up replaces
    assert 'SOURCE_IMAGE="localhost/crm-source:pg18-wal2json-$(sha256sum "$SOURCE_CONTAINERFILE" | cut -c1-12)"' in LOCAL_SH
    up = function("source_db_up")
    assert up.index('source_image "$e"') < up.index('!= "$SOURCE_IMAGE"') < up.index('"$e" rm -f crm-source')
    assert "volume rm" not in up            # the data stays


def test_the_slots_are_dropped_before_a_reload_and_created_after_the_seed_is_in():
    up = function("source_db_up")
    # The reload of fake_crm.sql is not a change to capture; the seed's own rows are not either
    assert (up.index("source_cdc_drop_slots") < up.index('source_psql < "$REPO/local/source_db/fake_crm.sql"')
            < up.index("\n    source_cdc_setup"))
    assert up.index("wait_for_postgres") < up.index("\n    source_cdc_setup")
    assert 'source_psql -d "$db" < "$REPO/local/source_cdc.sql"' in function("source_cdc_setup")
    assert "for db in $(extracted_databases)" in function("source_cdc_setup")
    setup = (REPO / "local" / "source_cdc.sql").read_text()
    assert "pg_create_logical_replication_slot('cdc_' || current_database(), 'wal2json')" in setup
    assert setup.count("WHERE NOT EXISTS") == 2         # safe to run again: the role, and the slot
    assert cdc.slot_name("crm_broomx") == "cdc_crm_broomx"


def test_status_shows_each_slot_s_lag_and_whether_it_still_has_its_log():
    slots = function("source_slots")
    for shown in ("pg_current_wal_lsn() - confirmed_flush_lsn", "wal_status", "invalidation_reason"):
        assert shown in slots
    status = LOCAL_SH[LOCAL_SH.index("\n    status)"):]
    assert 'echo "CDC slots:  $(source_slots' in status[:status.index(";;")]
