"""How the CRM databases are read: one consistent snapshot per load, and incremental reads that an
index on updated_at can serve."""
from types import SimpleNamespace

import psycopg

from crm_pipeline import source_postgres
from crm_pipeline.config import SourceTable
from crm_pipeline.connections import Connection


class FakeConnection(SimpleNamespace):
    """What connect() sets on a psycopg connection."""

    def __init__(self):
        super().__init__(isolation_level=None, read_only=None,
                         adapters=SimpleNamespace(register_loader=lambda *args: None))


def connect(monkeypatch, **kwargs):
    monkeypatch.setattr(source_postgres.connections, "get_connection",
                        lambda conn_id: Connection(conn_id=conn_id, host="db", login="u", password="p"))
    monkeypatch.setattr(source_postgres.psycopg, "connect", lambda **params: FakeConnection())
    return source_postgres.connect("crm_broomx", **kwargs)


def test_a_load_reads_every_table_in_one_read_only_snapshot(monkeypatch):
    conn = connect(monkeypatch, snapshot=True)
    assert conn.isolation_level == psycopg.IsolationLevel.REPEATABLE_READ
    assert conn.read_only is True


def test_other_connections_keep_the_server_defaults(monkeypatch):
    conn = connect(monkeypatch)                   # the simulator writes through these
    assert conn.isolation_level is None and conn.read_only is None


class RecordingCursor:
    def __init__(self, answer=None):
        self.answer, self.executed = answer, []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchmany(self, size):
        return []

    def fetchone(self):
        return self.answer


def test_changed_rows_are_read_by_updated_at_without_a_sort(monkeypatch):
    cursor = RecordingCursor()
    conn = SimpleNamespace(cursor=lambda name=None: cursor)
    columns = [source_postgres.SourceColumn("id", "integer", None, None)]
    list(source_postgres.iter_rows(conn, SourceTable("orders"), columns, 100, changed_after="2026-10-21"))
    sql, params = cursor.executed[0]
    assert sql == 'select "id" from "orders" where "updated_at" > %s'
    assert params == ("2026-10-21",)


def test_table_figures_are_a_row_count_and_an_updated_at_checksum():
    cursor = RecordingCursor(answer=(3157, 5_563_000_000_000))      # PostgreSQL returns the sum as numeric
    figures = source_postgres.table_figures(SimpleNamespace(cursor=lambda: cursor), SourceTable("orders"))
    assert figures == source_postgres.TableFigures(3157, 5_563_000_000_000)
    # bigint before the sum, so PostgreSQL before 14, whose extract returns a float, sums exactly
    assert 'sum(floor(extract(epoch from "updated_at"))::bigint)' in cursor.executed[0][0]
