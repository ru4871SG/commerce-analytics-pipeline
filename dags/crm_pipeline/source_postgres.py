"""Read the brands' CRM databases (Postgres)
"""
from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

import psycopg
from psycopg.types.string import TextLoader

from crm_pipeline import connections
from crm_pipeline.config import SOURCE_POSTGRES_CONN_ID, SourceTable
from crm_pipeline.errors import PermanentError


@dataclass(frozen=True)
class SourceColumn:
    name: str
    data_type: str          # information_schema data_type, e.g. "integer", "character varying"
    precision: int | None
    scale: int | None


def _identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def connect(database: str, conn_id: str = SOURCE_POSTGRES_CONN_ID, snapshot: bool = False) -> psycopg.Connection:
    """A connection to one brand's database. With snapshot, every statement until the connection
    closes reads the same moment of the database (REPEATABLE READ, read only): the tables of one
    load agree with each other, so an order line is never copied without its order, and the
    reconciliation figures describe exactly the rows that were copied."""
    conn = connections.get_connection(conn_id)
    connection = psycopg.connect(
        host=conn.host,
        port=conn.port or 5432,
        user=conn.login,
        password=conn.password,
        dbname=database,
        connect_timeout=60,
        **conn.extra,          # libpq parameters, e.g. {"sslmode": "require"}
    )
    if snapshot:
        connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        connection.read_only = True
    # json and jsonb arrive as the text the source holds, like every other text column
    connection.adapters.register_loader("json", TextLoader)
    connection.adapters.register_loader("jsonb", TextLoader)
    return connection


def check_source_tables(database: str, tables: Iterable[SourceTable]) -> None:
    """Fail fast, before anything is loaded, if the export is missing a table the models need."""
    names = [table.name for table in tables]
    with connect(database) as conn, conn.cursor() as cursor:
        cursor.execute("select table_name from information_schema.tables where table_schema = current_schema()")
        present = {row[0].lower() for row in cursor.fetchall()}
    missing = sorted(set(names) - present)
    if missing:
        raise PermanentError(f"{database} is missing tables the dbt models need: {', '.join(missing)}")


def table_columns(conn, table: SourceTable) -> list[SourceColumn]:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            select column_name, data_type, numeric_precision, numeric_scale
            from information_schema.columns
            where table_schema = current_schema() and table_name = %s
            order by ordinal_position
            """,
            (table.name,),
        )
        columns = [SourceColumn(r[0], r[1].lower(), r[2], r[3]) for r in cursor.fetchall()]
    return [column for column in columns if table.keeps(column.name)]


@dataclass(frozen=True)
class TableFigures:
    """What reconcile compares bronze with: the table's rows, and the sum of their updated_at in
    whole seconds since 1970. A row bronze holds in an older version lowers the sum, so a change
    that was never copied shows even when the row counts agree."""

    rows: int
    updated_at_checksum: int


def table_figures(conn, table: SourceTable) -> TableFigures:
    """The figures of the whole table, as the connection sees it. Read them on the connection that
    copied the table (connect(snapshot=True)): they then describe exactly the rows copied."""
    with conn.cursor() as cursor:
        cursor.execute(
            # bigint before the sum: before PostgreSQL 14 extract returns a float, which a sum
            # over millions of rows would round
            f'select count(*), coalesce(sum(floor(extract(epoch from "updated_at"))::bigint), 0) '
            f"from {_identifier(table.name)}"
        )
        rows, checksum = cursor.fetchone()
    return TableFigures(int(rows), int(checksum))


def iter_rows(conn, table: SourceTable, columns: list[SourceColumn], chunk_size: int,
              changed_after=None) -> Iterator[list[tuple]]:
    """Stream the table in chunks, so a large table never has to fit in memory. With
    changed_after, only the rows whose updated_at is later than it (an index range scan)."""
    column_list = ", ".join(_identifier(c.name) for c in columns)
    # A named cursor is a server-side cursor: each fetchmany brings the next chunk only
    with conn.cursor(name=f"copy_{table.name}") as cursor:
        if changed_after is None:
            cursor.execute(f"select {column_list} from {_identifier(table.name)}")
        else:
            cursor.execute(
                f'select {column_list} from {_identifier(table.name)} where "updated_at" > %s',
                (changed_after,),
            )
        while rows := cursor.fetchmany(chunk_size):
            yield rows
