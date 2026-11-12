"""Unit tests for the bronze loader, with Snowflake replaced by a recording cursor."""
from contextlib import nullcontext
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

import pytest

from crm_pipeline import bronze_loader
from crm_pipeline.config import INCREMENTAL_LOOKBACK, INCREMENTAL_TABLES, SOURCE_TABLES, Brand, SourceTable
from crm_pipeline.errors import PermanentError
from crm_pipeline.source_postgres import SourceColumn, TableFigures


def col(name, data_type, precision=None, scale=None):
    return SourceColumn(name, data_type, precision, scale)


TOKEN = UUID("6f1c8e52-2b4d-4c1a-9e57-0d3b8a1f4c2e")


@pytest.mark.parametrize("column, expected", [
    (col("id", "integer"), "NUMBER(38, 0)"),
    (col("position", "smallint"), "NUMBER(38, 0)"),
    (col("is_subscription", "boolean"), "BOOLEAN"),
    (col("price", "numeric", 10, 2), "NUMBER(10, 2)"),
    (col("average_rating", "double precision"), "FLOAT"),
    (col("created_at", "timestamp without time zone"), "TIMESTAMP_NTZ"),
    (col("paid_at", "timestamp with time zone"), "TIMESTAMP_TZ"),
    (col("birthday", "date"), "DATE"),
    (col("configuration", "jsonb"), "VARCHAR"),
    (col("notes", "text"), "VARCHAR"),
    (col("tier", "user-defined"), "VARCHAR"),
    (col("blob_col", "bytea"), "BINARY"),
])
def test_postgres_types_map_to_snowflake(column, expected):
    assert bronze_loader.snowflake_type(column) == expected


def test_frame_keeps_integer_columns_integer_when_they_have_nulls():
    columns = [col("id", "integer"), col("customer_id", "integer"), col("token", "uuid"), col("flag", "boolean")]
    frame = bronze_loader.to_frame([(1, None, TOKEN, True), (2, 7, None, False)], columns)
    assert list(frame.columns) == ["ID", "CUSTOMER_ID", "TOKEN", "FLAG"]
    assert str(frame["CUSTOMER_ID"].dtype) == "Int64"
    assert frame["CUSTOMER_ID"].tolist()[1] == 7
    assert frame["TOKEN"].tolist()[0] == str(TOKEN)
    assert frame["FLAG"].tolist() == [True, False]


def test_frame_keeps_large_integers_exact_next_to_nulls():
    # pandas makes a column with a NULL in it float64, which cannot hold 2**53 + 1
    big = 2**53 + 1
    frame = bronze_loader.to_frame([(big,), (None,)], [col("id", "bigint")])
    assert frame["ID"].tolist()[0] == big
    assert frame["ID"].isna().tolist() == [False, True]


def test_sensitive_columns_are_never_copied():
    tables = {t.name: t for t in SOURCE_TABLES}
    assert not tables["customers"].keeps("email")
    assert tables["customers"].keeps("country_code")
    assert tables["orders"].keeps("total")
    assert tables["refund_payments"].keeps("amount")


def test_load_method_comes_from_the_connection_extra(monkeypatch):
    conn = bronze_loader.connections.Connection(conn_id="snowflake_default", extra={"load_method": "insert"})
    monkeypatch.setattr(bronze_loader.connections, "get_connection", lambda conn_id: conn)
    assert bronze_loader.load_method() == "insert"

    conn = bronze_loader.connections.Connection(conn_id="snowflake_default", extra={})
    monkeypatch.setattr(bronze_loader.connections, "get_connection", lambda conn_id: conn)
    assert bronze_loader.load_method() == "write_pandas"


def test_insert_rows_turn_text_types_into_strings_and_keep_nulls():
    columns = [col("id", "integer"), col("flag", "boolean"), col("token", "uuid")]
    rows = bronze_loader.to_insert_rows([(1, True, TOKEN), (2, None, None)], columns)
    assert rows == [(1, True, str(TOKEN)), (2, None, None)]


class RecordingCursor:
    """Answers the two row counts publish_brand_slice asks for and records every statement."""

    def __init__(self, staged, live, fail_on=None):
        self.counts = [staged, live]
        self.fail_on = fail_on
        self.statements = []
        self.executed = []                        # (sql, params) of every statement

    def execute(self, sql, params=None):
        self.statements.append(sql.split()[0].lower())
        self.executed.append((sql, params))
        if self.fail_on and sql.lower().startswith(self.fail_on):
            raise RuntimeError("warehouse error")

    def fetchone(self):
        return (self.counts.pop(0),)


COLUMNS = [col("id", "integer"), col("total", "integer")]


def test_brand_slice_is_replaced_in_one_transaction():
    cursor = RecordingCursor(staged=9, live=8)
    loaded = bronze_loader.publish_brand_slice(cursor, "orders", "orders__load_broomx", COLUMNS, "broomx", "2026-03-31 06:00:00", expected_rows=9)
    assert loaded == 9
    assert cursor.statements == ["select", "select", "begin", "delete", "insert", "commit"]


def test_a_duplicated_or_lost_batch_is_never_published():
    # e.g. a retried INSERT that the warehouse ran twice
    cursor = RecordingCursor(staged=18, live=8)
    with pytest.raises(bronze_loader.StagedRowCountError):
        bronze_loader.publish_brand_slice(cursor, "orders", "orders__load_broomx", COLUMNS, "broomx", "2026-03-31 06:00:00", expected_rows=9)
    assert "delete" not in cursor.statements


def test_an_empty_export_does_not_wipe_bronze():
    cursor = RecordingCursor(staged=0, live=8)
    with pytest.raises(bronze_loader.EmptyExtractError):
        bronze_loader.publish_brand_slice(cursor, "orders", "orders__load_broomx", COLUMNS, "broomx", "2026-03-31 06:00:00")
    assert "delete" not in cursor.statements


def test_a_new_brand_may_start_empty():
    cursor = RecordingCursor(staged=0, live=0)
    assert bronze_loader.publish_brand_slice(cursor, "processing_costs", "s", COLUMNS, "broomx", "2026-03-31 06:00:00") == 0


def test_a_failed_insert_rolls_back():
    cursor = RecordingCursor(staged=9, live=8, fail_on="insert")
    with pytest.raises(RuntimeError):
        bronze_loader.publish_brand_slice(cursor, "orders", "s", COLUMNS, "broomx", "2026-03-31 06:00:00")
    assert cursor.statements[-1] == "rollback"
    assert "commit" not in cursor.statements


def test_a_new_source_column_is_added_to_bronze_and_to_the_catalog():
    catalog = {"ORDERS": ["ID", "_BRAND", "_LOADED_AT"]}
    cursor = RecordingCursor(staged=0, live=0)
    bronze_loader.ensure_bronze_table(cursor, catalog, "orders", COLUMNS)
    assert [sql for sql, _ in cursor.executed] == ['alter table RAW.CRM."ORDERS" add column "TOTAL" NUMBER(38, 0)']
    assert "TOTAL" in catalog["ORDERS"]                  # the next table needs no query
    bronze_loader.ensure_bronze_table(cursor, catalog, "orders", COLUMNS)
    assert len(cursor.executed) == 1


class LoadCursor(RecordingCursor):
    """A cursor for load_table: the catalog shows bronze holding the tables with the source's
    columns; executemany is recorded. versions: the (id, updated_at) the change log answers
    loaded_versions with."""

    def __init__(self, staged, live, columns=None, versions=(), tables=("orders", "orders_changes")):
        super().__init__(staged, live)
        self.columns = columns or COLUMNS
        self.versions = list(versions)
        self.tables = tables
        self.batches = []

    def executemany(self, sql, rows):
        self.batches.append(len(rows))

    def fetchall(self):
        if self.executed[-1][0].startswith('select "ID", "UPDATED_AT"'):
            return self.versions
        return [(table.upper(), c.name.upper()) for table in self.tables for c in self.columns]

    def close(self):
        pass


FIGURES = TableFigures(rows=3157, updated_at_checksum=5_563_000_000_000)
CHANGED_COLUMNS = [col("id", "integer"), col("total", "integer"), col("updated_at", "timestamp without time zone")]
SINCE = datetime(2026, 10, 21, 7, 0)


def load_orders(monkeypatch, source_rows, staged, table=SourceTable("orders"), watermark=None, appended=None,
                versions=(), load_method="insert"):
    """load_table on orders. For an incremental table, versions are the change-log rows inside
    the lookback; staged is what the staging table answers when counted."""
    reads = []
    columns = CHANGED_COLUMNS if table.incremental else COLUMNS

    def iter_rows(conn, table, columns, size, changed_after=None):
        reads.append(changed_after)
        return iter([source_rows])

    monkeypatch.setattr(bronze_loader.source_postgres, "table_columns", lambda conn, table: columns)
    monkeypatch.setattr(bronze_loader.source_postgres, "iter_rows", iter_rows)
    monkeypatch.setattr(bronze_loader.source_postgres, "table_figures", lambda conn, table: FIGURES)
    cursor = LoadCursor(staged=staged, live=0, columns=columns, versions=versions)
    if table.incremental:
        # the watermark, the staged count, then the rows the INSERT appended
        cursor.counts = [watermark, staged, staged if appended is None else appended]
    load = bronze_loader.load_table(None, SimpleNamespace(cursor=lambda: cursor), "broomx", table,
                                    "2026-03-31 06:00:00", 50_000, load_method=load_method)
    cursor.reads = reads
    cursor.load = load
    return load.rows, cursor


def changed_rows(n, updated_at=SINCE):
    return [(i, 100, updated_at) for i in range(n)]


def statements_on(cursor, table):
    """The statements that name RAW.CRM.<table>, verb first."""
    name = f'RAW.CRM."{table.upper()}"'
    return [sql.split()[0].lower() for sql, _ in cursor.executed if name in sql]


def test_insert_path_sends_small_batches(monkeypatch):
    loaded, cursor = load_orders(monkeypatch, [(i, 100) for i in range(1200)], staged=1200)
    assert loaded == 1200
    assert cursor.batches == [500, 500, 200]
    assert cursor.statements[-1] == "drop"        # the staging table is always removed


def test_small_chunks_go_in_as_inserts_and_large_ones_through_write_pandas(monkeypatch):
    frames = []
    monkeypatch.setattr(bronze_loader, "write_pandas", lambda conn, frame, **kwargs: frames.append(len(frame)))
    monkeypatch.setattr(bronze_loader, "WRITE_PANDAS_MIN_ROWS", 100)
    _, cursor = load_orders(monkeypatch, [(i, 100) for i in range(99)], staged=99, load_method="write_pandas")
    assert (cursor.batches, frames) == ([99], [])        # a stage, a PUT and a COPY cost more
    _, cursor = load_orders(monkeypatch, [(i, 100) for i in range(150)], staged=150, load_method="write_pandas")
    assert (cursor.batches, frames) == ([], [150])


def test_a_table_bronze_holds_is_not_created_again(monkeypatch):
    _, cursor = load_orders(monkeypatch, [(1, 100)], staged=1)
    assert "create" not in statements_on(cursor, "orders")      # only the staging table is created
    assert not [sql for sql, _ in cursor.executed if sql.startswith(("create schema", "alter"))]


def test_load_table_refuses_a_staging_table_with_extra_rows(monkeypatch):
    with pytest.raises(bronze_loader.StagedRowCountError):
        load_orders(monkeypatch, [(i, 100) for i in range(1200)], staged=2400)


def test_incremental_tables_copy_only_changed_rows_and_append_them(monkeypatch):
    loaded, cursor = load_orders(monkeypatch, changed_rows(12), staged=12,
                                 table=SourceTable("orders", incremental=True), watermark=SINCE)
    assert loaded == 12
    assert "delete" not in statements_on(cursor, "orders")          # nothing replaced in bronze
    assert statements_on(cursor, "orders_changes").count("insert") == 1


def test_an_incremental_load_reads_again_a_lookback_before_the_watermark(monkeypatch):
    # A transaction stamps updated_at when it starts and commits later: rows committed after the
    # last load can carry an updated_at below its watermark
    _, cursor = load_orders(monkeypatch, changed_rows(1), staged=1, table=SourceTable("orders", incremental=True),
                            watermark=SINCE)
    assert cursor.reads == [SINCE - INCREMENTAL_LOOKBACK]


def test_rows_read_again_that_bronze_already_has_are_not_staged(monkeypatch):
    rows = changed_rows(12)
    loaded, cursor = load_orders(monkeypatch, rows, staged=3, table=SourceTable("orders", incremental=True),
                                 watermark=SINCE, versions=[(r[0], r[2]) for r in rows[:9]], appended=3)
    assert cursor.batches == [3]                  # the 9 versions bronze holds never reach staging
    assert loaded == 3


def test_a_version_bronze_has_in_another_state_is_staged(monkeypatch):
    # Same id, a later updated_at: the change committed late, so it is new to bronze
    late = datetime(2026, 10, 21, 6, 30)
    _, cursor = load_orders(monkeypatch, [(5, 100, SINCE)], staged=1, table=SourceTable("orders", incremental=True),
                            watermark=SINCE, versions=[(5, late)])
    assert cursor.batches == [1]


def test_the_insert_also_drops_versions_bronze_holds(monkeypatch):
    # When the lookback holds too many versions to fetch, the warehouse drops them instead
    loaded, cursor = load_orders(monkeypatch, changed_rows(12), staged=12,
                                 table=SourceTable("orders", incremental=True), watermark=SINCE, appended=3)
    assert loaded == 3                            # the versions the INSERT appended, not the rows read
    sql, params = next((sql, params) for sql, params in cursor.executed
                       if sql.startswith('insert into RAW.CRM."ORDERS_CHANGES"'))
    assert "not exists" in sql and '"UPDATED_AT" = s."UPDATED_AT"' in sql
    assert params[-1] == SINCE - INCREMENTAL_LOOKBACK     # older versions are not searched


def test_too_many_versions_in_the_lookback_are_left_to_the_warehouse(monkeypatch):
    monkeypatch.setattr(bronze_loader, "PREFILTER_MAX_VERSIONS", 2)
    cursor = LoadCursor(staged=0, live=0, versions=[(1, SINCE), (2, SINCE), (3, SINCE)])
    assert bronze_loader.loaded_versions(cursor, "orders", "broomx", SINCE) is None
    sql, _ = cursor.executed[0]
    assert len(cursor.executed) == 1 and sql.endswith("limit 3")     # one query, at most one row too many


def test_an_incremental_load_takes_the_source_figures_of_its_snapshot(monkeypatch):
    _, cursor = load_orders(monkeypatch, changed_rows(1), staged=1, table=SourceTable("orders", incremental=True),
                            watermark=SINCE)
    assert cursor.load.figures == FIGURES
    assert statements_on(cursor, "_source_figures") == []       # load_brand records them, once per brand


def test_a_full_copy_takes_no_figures(monkeypatch):
    _, cursor = load_orders(monkeypatch, [(1, 100)], staged=1)
    assert cursor.load.figures is None


def test_the_first_incremental_load_copies_everything(monkeypatch):
    _, cursor = load_orders(monkeypatch, changed_rows(1), staged=1, table=SourceTable("orders", incremental=True),
                            watermark=datetime(1970, 1, 1))     # fakesnow's NULL timestamp
    assert cursor.reads == [None]


class CatalogCursor(RecordingCursor):
    """Answers the catalog query with bronze holding every table, _SOURCE_FIGURES included."""

    def fetchall(self):
        return [(table, "ID") for table in ("CUSTOMERS", "ORDERS", "PRODUCTS", "_SOURCE_FIGURES")]

    def close(self):
        pass


def test_a_brand_load_reads_the_catalog_once_and_records_every_figure_in_one_transaction(monkeypatch):
    tables = (SourceTable("customers", incremental=True), SourceTable("orders", incremental=True),
              SourceTable("products"))
    loads = iter([bronze_loader.TableLoad(3, TableFigures(10, 100)), bronze_loader.TableLoad(4, TableFigures(20, 200)),
                  bronze_loader.TableLoad(5)])
    catalogs, snapshots = [], []

    def load_table(source, snowflake, brand, table, loaded_at, chunk_size, load_method, catalog):
        catalogs.append(catalog)
        return next(loads)

    def connect(database, snapshot=False):
        snapshots.append(snapshot)
        return nullcontext()

    monkeypatch.setattr(bronze_loader, "load_table", load_table)
    monkeypatch.setattr(bronze_loader.source_postgres, "connect", connect)
    cursor = CatalogCursor(staged=0, live=0)
    counts = bronze_loader.load_brand(SimpleNamespace(cursor=lambda: cursor), "broomx", "crm_broomx", tables,
                                      load_method="insert")
    assert counts == {"customers": 3, "orders": 4, "products": 5}
    assert snapshots == [True]                                   # nine tables, one snapshot
    assert len({id(catalog) for catalog in catalogs}) == 1       # read once, shared by every table
    assert len([sql for sql, _ in cursor.executed if "information_schema" in sql]) == 1
    assert statements_on(cursor, "_source_figures") == ["delete", "insert"]    # it exists: not created
    assert cursor.statements[-4:] == ["begin", "delete", "insert", "commit"]
    inserted = next(params for sql, params in cursor.executed if sql.startswith('insert into RAW.CRM."_SOURCE_FIGURES"'))
    assert inserted[:4] == ("broomx", "customers", 10, 100) and inserted[5:9] == ("broomx", "orders", 20, 200)


def test_incremental_tables_are_the_ones_that_change():
    assert {t.name for t in INCREMENTAL_TABLES} == {"customers", "orders", "order_items", "refund_payments",
                                                    "marketing_spend"}


class FiguresCursor:
    """Answers reconcile: the recorded figures, then bronze's, one list of rows per query."""

    def __init__(self, *answers):
        self.answers = list(answers)
        self.executed = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.answers.pop(0)

    def close(self):
        pass


BROOMX = Brand("broomx", "Broomx", "crm_broomx", True)
GREENZ = Brand("greenz", "Greenz", "crm_greenz", True)
ORDERS = SourceTable("orders", incremental=True)
CUSTOMERS = SourceTable("customers", incremental=True)


def reconcile(monkeypatch, recorded, bronze, resync=False, figures_after_resync=None, bronze_after_resync=None):
    """reconcile for broomx's orders. recorded and bronze: the (rows, checksum) the two queries
    answer; recorded None when the load recorded nothing. Returns the results, the tables
    copied whole and the source connections opened."""
    loads, connections = [], []

    def connect(database, snapshot=False):
        connections.append(snapshot)
        return nullcontext()

    monkeypatch.setattr(bronze_loader.source_postgres, "connect", connect)
    monkeypatch.setattr(bronze_loader.source_postgres, "table_figures", lambda conn, table: figures_after_resync)
    monkeypatch.setattr(bronze_loader, "load_table", lambda source, snowflake, brand, table, *args: loads.append(table))
    answers = [[("broomx", "orders", *recorded)] if recorded else [], [("orders", "broomx", *bronze)]]
    if bronze_after_resync:
        answers.append([("orders", "broomx", *bronze_after_resync)])
    cursor = FiguresCursor(*answers)
    results = bronze_loader.reconcile(SimpleNamespace(cursor=lambda: cursor), [BROOMX], tables=(ORDERS,),
                                      resync=resync)
    return results, loads, connections


def test_bronze_that_matches_the_snapshot_of_its_load_passes_without_reading_the_source(monkeypatch):
    results, loads, connections = reconcile(monkeypatch, (120, 9_000), (120, 9_000))
    assert results[0].matches
    assert results[0].bronze_rows == 120
    assert loads == [] and connections == []


def test_every_brand_and_table_is_compared_in_two_queries():
    cursor = FiguresCursor(
        [("broomx", "orders", 1, 10), ("broomx", "customers", 2, 20), ("greenz", "orders", 3, 30),
         ("greenz", "customers", 0, 0)],
        # greenz has no customer in bronze: the group by has no row for it, which counts as none
        [("orders", "broomx", 1, 10), ("customers", "broomx", 2, 20), ("orders", "greenz", 3, 30)],
    )
    results = bronze_loader.reconcile(SimpleNamespace(cursor=lambda: cursor), [BROOMX, GREENZ], tables=(ORDERS, CUSTOMERS))
    assert [(r.brand, r.table, r.matches) for r in results] == [
        ("broomx", "orders", True), ("broomx", "customers", True), ("greenz", "orders", True), ("greenz", "customers", True),
    ]
    assert [sql.split()[0] for sql, _ in cursor.executed] == ["create", "select", "select"]
    assert cursor.executed[2][0].count("union all") == 1 and "group by _BRAND" in cursor.executed[2][0]


def test_a_row_deleted_in_the_source_is_reported_not_repaired(monkeypatch):
    results, loads, _ = reconcile(monkeypatch, (119, 8_900), (120, 9_000))
    assert not results[0].matches
    assert "119 rows in the source, 120 in bronze" in str(results[0])
    assert loads == []                            # without resync, bronze is left alone


def test_a_change_never_copied_is_found_although_the_counts_agree(monkeypatch):
    # The source cancelled an order; bronze still holds the version before, with an older updated_at
    results, _, _ = reconcile(monkeypatch, (120, 9_000), (120, 8_400))
    assert not results[0].matches
    assert "checksums differ" in str(results[0])


def test_reconcile_needs_the_figures_of_a_load(monkeypatch):
    with pytest.raises(PermanentError):
        reconcile(monkeypatch, None, (120, 9_000))


def test_resync_copies_the_table_whole_in_a_snapshot_and_compares_again(monkeypatch):
    after = TableFigures(119, 8_900)
    results, loads, connections = reconcile(
        monkeypatch, (120, 9_000), (119, 8_950), resync=True, figures_after_resync=after, bronze_after_resync=(119, 8_900),
    )
    assert [table.incremental for table in loads] == [False]     # the reference tables' full copy
    assert connections == [True]                                 # one read-only snapshot
    assert results[0].matches and results[0].resynced
    assert results[0].source == after


def test_the_mismatch_message_says_how_to_resync():
    difference = bronze_loader.Reconciliation("orders", "broomx", TableFigures(119, 1), TableFigures(120, 1))
    message = bronze_loader.mismatch_message([difference], "run it with resync")
    assert message.startswith("Bronze does not match the source:\n  orders for broomx: 119 rows in the source")
    assert message.endswith("run it with resync to copy these tables whole.")
