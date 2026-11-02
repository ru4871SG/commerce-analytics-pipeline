"""The stream and task stand-in, with Snowflake replaced by a cursor that answers its queries."""
from datetime import datetime

import pytest

from crm_pipeline import stream_task

LOADED = datetime(2026, 10, 22, 11, 2)
CHANGE_COLUMNS = ("ID", "STATE", "UPDATED_AT", "_BRAND", "_LOADED_AT")


class StreamCursor:
    """Answers the queries the stream and task send and records the statement verbs.
    waiting: the change rows loaded after the offset, all of them broomx's."""

    def __init__(self, offset=None, waiting=0, fail_merge=False):
        self.offset, self.waiting, self.fail_merge = offset, waiting, fail_merge
        self.statements = []
        self.executed = []
        self.history = []
        self._rows = []

    def execute(self, sql, params=None):
        verb = sql.split()[0].lower()
        self.statements.append(verb)
        self.executed.append((sql, params))
        if "max(OFFSET_LOADED_AT)" in sql:
            self._rows = [(self.offset,)]
        elif sql.startswith("select _BRAND, count(*), max(_LOADED_AT)"):
            self._rows = [("broomx", self.waiting, LOADED)] if self.waiting else []
        elif "count_if(t._BRAND is null) over ()" in sql:
            # one new row and one changed one, with their state before and after
            self._rows = [("broomx", 7, False, "paid", "new", 1, 1), ("broomx", 9, True, "new", None, 1, 1)]
        elif sql.startswith("select table_name, column_name"):
            self._rows = [("ORDERS_CHANGES", c) for c in CHANGE_COLUMNS]
        elif "information_schema.columns" in sql:
            self._rows = [(c,) for c in CHANGE_COLUMNS]
        elif verb == "merge":
            if self.fail_merge:
                raise RuntimeError("warehouse error")
            self._rows = [(2, 1)]
        elif verb == "insert" and "_task_history" in sql.lower():
            self.history.append(params[1])
        else:
            self._rows = []

    def fetchone(self):
        return self._rows[0]

    def fetchall(self):
        return self._rows


def test_names_follow_snowflake():
    assert stream_task.stream_name("orders") == "ORDERS_CHANGES_STREAM"
    assert stream_task.task_name("orders") == "MERGE_ORDERS"


def test_the_change_log_prints_values_as_sql_would():
    assert [stream_task._show(v) for v in (False, True, None, "paid", 4999)] == ["false", "true", "null", "'paid'", "4999"]
    assert stream_task._show(datetime(2026, 10, 21, 21, 36, 39)) == "2026-10-21 21:36:39"


def test_a_task_whose_stream_has_no_data_is_skipped():
    cursor = StreamCursor(offset=LOADED, waiting=0)
    run = stream_task.run_merge_task(cursor, "orders", "run-1", "airflow")
    assert run.state == "SKIPPED"
    assert "merge" not in cursor.statements
    assert cursor.history == ["SKIPPED"]


def test_the_task_merges_then_consumes_the_stream():
    cursor = StreamCursor(offset=None, waiting=3)
    run = stream_task.run_merge_task(cursor, "orders", "run-2", "dagster")
    assert (run.state, run.inserted, run.updated) == ("SUCCEEDED", 2, 1)
    merge = cursor.statements.index("merge")
    # The offset moves in its own transaction, after the MERGE
    assert cursor.statements[merge + 1:merge + 5] == ["begin", "delete", "insert", "commit"]
    assert cursor.history == ["SUCCEEDED"]


def test_a_failed_merge_leaves_the_stream_unconsumed():
    cursor = StreamCursor(offset=None, waiting=3, fail_merge=True)
    with pytest.raises(RuntimeError):
        stream_task.run_merge_task(cursor, "orders", "run-3", "dagster")
    assert "begin" not in cursor.statements          # the offset was not moved
    assert cursor.history == ["FAILED"]


def test_reading_the_stream_counts_and_samples_its_rows_in_two_queries_after_the_offset():
    cursor = StreamCursor(offset=None, waiting=3)
    catalog = stream_task.ensure_metadata(cursor)
    before = len(cursor.executed)
    changes = stream_task.read_stream(cursor, "orders", catalog)
    assert len(cursor.executed) - before == 3        # the offset, the counts, the sample
    assert (changes.rows, changes.by_brand, changes.through) == (3, {"broomx": 3}, LOADED)
    assert (changes.new, changes.changed) == (1, 1)
    assert changes.samples == ["broomx id 7: state 'new' -> 'paid'", "broomx id 9: new row"]


def test_the_task_merges_what_the_stream_read_found_and_no_more():
    cursor = StreamCursor(offset=None, waiting=3)
    changes = stream_task.read_stream(cursor, "orders", stream_task.ensure_metadata(cursor))
    before = len(cursor.executed)
    run = stream_task.run_merge_task(cursor, "orders", "run-4", "airflow", changes)
    assert run.state == "SUCCEEDED"
    # Nothing is read again: the MERGE, the offset's transaction and the history row
    assert cursor.statements[before:] == ["merge", "begin", "delete", "insert", "commit", "insert"]
    merge_sql, merge_params = next((sql, params) for sql, params in cursor.executed if sql.startswith("merge"))
    assert "_LOADED_AT <= %s" in merge_sql and merge_params == (LOADED,)
    offset = next(params for sql, params in cursor.executed if sql.startswith('insert into RAW.CRM."_STREAM_OFFSETS"'))
    assert offset[2] == LOADED                       # consumed up to the latest change the read saw


class MetadataCursor:
    """An old _TASK_HISTORY table: it has AIRFLOW_RUN_ID but not RUN_ID/ORCHESTRATOR."""

    def __init__(self):
        self.statements = []
        self._rows = []

    def execute(self, sql, params=None):
        self.statements.append(sql)
        if "information_schema.columns" in sql:
            self._rows = [("_TASK_HISTORY", column) for column in (
                "NAME", "STATE", "ROWS_INSERTED", "ROWS_UPDATED", "OFFSET_FROM", "OFFSET_TO",
                "COMPLETED_TIME", "AIRFLOW_RUN_ID", "ERROR_MESSAGE",
            )] + [("_STREAM_OFFSETS", "STREAM_NAME")]

    def fetchall(self):
        return self._rows


def test_an_old_task_history_table_gets_the_new_columns():
    cursor = MetadataCursor()
    stream_task.ensure_metadata(cursor)
    alters = [sql for sql in cursor.statements if sql.lower().startswith("alter table")]
    assert any('"RUN_ID"' in sql and "VARCHAR" in sql for sql in alters)
    assert any('"ORCHESTRATOR"' in sql and "VARCHAR" in sql for sql in alters)
    # The existing Airflow column stays; new rows write NULL there.
    assert not any("DROP COLUMN" in sql.upper() for sql in cursor.statements)


def test_metadata_that_exists_is_not_created_again():
    cursor = MetadataCursor()
    stream_task.ensure_metadata(cursor)
    # One catalog query and the session's schema; the tables are there, so no CREATE
    assert not [sql for sql in cursor.statements if sql.lower().startswith("create")]
    assert sum("information_schema" in sql for sql in cursor.statements) == 1
