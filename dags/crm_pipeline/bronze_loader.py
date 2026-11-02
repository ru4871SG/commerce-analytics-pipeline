"""Copy a brand's CRM tables into Snowflake's bronze layer, RAW.CRM.

All brands share one table per source table, with two columns added: _BRAND and _LOADED_AT.
A brand's nine tables are read in one snapshot of its database (source_postgres.connect(snapshot=True)),
so they agree with each other: an order line is never copied without its order or its product
variant. What RAW.CRM holds is read once per load (bronze_schema.read_catalog), and a table is
created or widened only when it lacks something.
"""
from __future__ import annotations

import contextlib
import dataclasses
import logging
from datetime import datetime, timezone

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

from crm_pipeline import connections, source_postgres
from crm_pipeline.bronze_schema import (
    changes_table, identifier, qualified, read_catalog, timestamp_or_none, use_schema,
)
from crm_pipeline.config import (
    BRONZE_DATABASE, BRONZE_SCHEMA, INCREMENTAL_LOOKBACK, INCREMENTAL_TABLES, SNOWFLAKE_CONN_ID, SOURCE_TABLES,
    SourceTable,
)
from crm_pipeline.errors import PermanentError
from crm_pipeline.source_postgres import SourceColumn, TableFigures

log = logging.getLogger(__name__)

SOURCE_FIGURES = "_source_figures"          # per brand and incremental table, as of its last load

# information_schema data_type values. "user-defined" is an enum; its values are text.
INTEGER_TYPES = {"smallint", "integer", "bigint"}
TEXT_TYPES = {"character", "character varying", "text", "json", "jsonb", "xml", "uuid", "user-defined", "array",
              "time without time zone", "time with time zone", "interval"}

# Rows per INSERT statement on the insert path. Small statements stay well inside the
# connector's request timeout even when several brands load at once.
INSERT_BATCH_ROWS = 500

# On the write_pandas path, a chunk with fewer rows than this goes in as INSERT statements: for
# every chunk write_pandas creates a stage, uploads a file and runs COPY, which costs more than
# a few small INSERTs. A day's changes and the reference tables are usually far below it.
WRITE_PANDAS_MIN_ROWS = 2_000

# Up to this many versions in the lookback window, the loader fetches their keys and drops the
# rows read again before staging them. Past it, the INSERT's anti-join drops them in the
# warehouse instead, which keeps the set small (and the emulator returns 2,048 rows at most).
PREFILTER_MAX_VERSIONS = 1000


class EmptyExtractError(Exception):
    """The source table came back empty although bronze has rows for the brand."""


class StagedRowCountError(Exception):
    """The staging table does not hold exactly the rows read from the source."""


def _private_key(extra: dict):
    """The PEM private key in the connection extra, as the connector's DER bytes."""
    content = extra.get("private_key_content")
    if not content:
        return None
    from cryptography.hazmat.primitives import serialization

    passphrase = extra.get("private_key_passphrase")
    key = serialization.load_pem_private_key(
        content.encode(),
        password=passphrase.encode() if passphrase else None,
    )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def snowflake_connection(conn_id: str = SNOWFLAKE_CONN_ID):
    """A Snowflake connection from an Airflow connection, or from the environment without Airflow.

    The Airflow Snowflake hook ignores host, port and protocol, which the local fakesnow
    emulator needs. When the connection has a host in its extra, connect directly instead.
    The connector re-sends a request that has not answered within socket_timeout; Snowflake
    ignores the repeat, fakesnow runs it again. A long timeout keeps slow emulator statements
    from being sent twice.
    """
    conn = connections.get_connection(conn_id)
    extra = conn.extra
    if extra.get("host") or not connections.airflow_installed():
        import snowflake.connector

        kwargs = {
            "user": conn.login,
            "password": conn.password,
            "account": extra.get("account", "fakesnow"),
            "database": extra.get("database"),
            "warehouse": extra.get("warehouse"),
            "role": extra.get("role"),
            "socket_timeout": int(extra.get("socket_timeout", 900)),
        }
        if extra.get("host"):
            kwargs.update(
                host=extra["host"],
                port=int(extra.get("port", 443)),
                protocol=extra.get("protocol", "https"),
            )
        private_key = _private_key(extra)
        if private_key:
            kwargs["private_key"] = private_key
        return snowflake.connector.connect(**{key: value for key, value in kwargs.items() if value is not None})

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=conn_id).get_conn()


@contextlib.contextmanager
def snowflake_session(conn_id: str = SNOWFLAKE_CONN_ID):
    """A Snowflake connection that is closed when the block ends, however it ends."""
    conn = snowflake_connection(conn_id)
    try:
        yield conn
    finally:
        conn.close()


def load_method(conn_id: str = SNOWFLAKE_CONN_ID) -> str:
    """How rows are written to Snowflake: write_pandas (real Snowflake) or insert (emulator)."""
    try:
        extra = connections.get_connection(conn_id).extra
    except Exception:
        return "write_pandas"
    return extra.get("load_method", "write_pandas")


def to_insert_rows(rows: list[tuple], columns: list[SourceColumn]) -> list[tuple]:
    """Rows as values the Snowflake connector can bind, without pandas' NaN and numpy types."""
    converted = []
    for row in rows:
        values = []
        for column, value in zip(columns, row):
            if value is None:
                values.append(None)
            elif column.data_type in TEXT_TYPES and not isinstance(value, str):
                values.append(str(value))
            elif isinstance(value, (bytes, bytearray, memoryview)):
                values.append(bytes(value))
            else:
                values.append(value)
        converted.append(tuple(values))
    return converted


def snowflake_type(column: SourceColumn) -> str:
    """The Snowflake type a PostgreSQL column is loaded into."""
    t = column.data_type
    if t in INTEGER_TYPES:
        return "NUMBER(38, 0)"
    if t == "numeric":
        return f"NUMBER({column.precision or 38}, {column.scale or 0})"
    if t in ("real", "double precision"):
        return "FLOAT"
    if t == "date":
        return "DATE"
    if t == "timestamp without time zone":
        return "TIMESTAMP_NTZ"
    if t == "timestamp with time zone":
        return "TIMESTAMP_TZ"
    if t == "boolean":
        return "BOOLEAN"
    if t == "bytea":
        return "BINARY"
    return "VARCHAR"        # TEXT_TYPES, and anything unexpected


def to_frame(rows: list[tuple], columns: list[SourceColumn]) -> pd.DataFrame:
    """Rows from PostgreSQL as a DataFrame whose dtypes write_pandas loads into snowflake_type()."""
    frame = pd.DataFrame.from_records(rows, columns=[c.name.upper() for c in columns])
    for position, column in enumerate(columns):
        name = column.name.upper()
        if column.data_type in INTEGER_TYPES:
            # From the rows, not the frame: pandas has made a column with a NULL in it float64,
            # which rounds integers above 2**53
            frame[name] = pd.array([row[position] for row in rows], dtype="Int64")
        elif column.data_type in TEXT_TYPES:
            frame[name] = frame[name].map(lambda v: v if v is None or isinstance(v, str) else str(v))
    return frame


def ensure_bronze_table(cursor, catalog: dict[str, list[str]], table: str, columns: list[SourceColumn]) -> None:
    """Create RAW.CRM.<TABLE> if the catalog does not hold it, and add any source column it
    lacks. The catalog is kept in step, so the next table needs no query."""
    if table.upper() not in catalog:
        definitions = ", ".join(f"{identifier(c.name)} {snowflake_type(c)}" for c in columns)
        cursor.execute(
            f"create table if not exists {qualified(table)} "
            f"({definitions}, _BRAND VARCHAR, _LOADED_AT TIMESTAMP_NTZ)"
        )
        # Another brand's load may have created it meanwhile, from a source with other columns
        catalog.update(read_catalog(cursor))
    existing = catalog[table.upper()]
    for column in columns:
        if column.name.upper() not in existing:
            log.info("Adding new column %s.%s", table, column.name)
            cursor.execute(f"alter table {qualified(table)} add column {identifier(column.name)} {snowflake_type(column)}")
            existing.append(column.name.upper())


def watermark(cursor, table: str, brand: str):
    """The latest updated_at already copied for the brand, or None before its first load."""
    cursor.execute(
        f'select max("UPDATED_AT") from {qualified(changes_table(table))} where _BRAND = %s', (brand,)
    )
    return timestamp_or_none(cursor.fetchone()[0])


def loaded_versions(cursor, table: str, brand: str, since) -> set[tuple] | None:
    """The (id, updated_at) of the brand's versions in the change log after `since`: the rows
    the lookback reads again that bronze already has. None when there are more than
    PREFILTER_MAX_VERSIONS."""
    cursor.execute(
        f'select "ID", "UPDATED_AT" from {qualified(changes_table(table))} '
        f'where _BRAND = %s and "UPDATED_AT" > %s limit {PREFILTER_MAX_VERSIONS + 1}',
        (brand, since),
    )
    rows = cursor.fetchall()
    if len(rows) > PREFILTER_MAX_VERSIONS:
        return None
    return {(int(row_id), updated_at) for row_id, updated_at in rows}


def _staged_count(cursor, table: str, staging: str, brand: str, expected_rows: int | None) -> int:
    """Rows in the staging table. expected_rows is how many were read from the source and
    written to staging; the staged count must match it."""
    cursor.execute(f"select count(*) from {qualified(staging)}")
    staged = cursor.fetchone()[0]
    if expected_rows is not None and staged != expected_rows:
        raise StagedRowCountError(
            f"{table} for {brand}: read {expected_rows} rows from the source but staged {staged}. Nothing was replaced."
        )
    return staged


def append_changes(cursor, table: str, staging: str, columns: list[SourceColumn], brand: str,
                   loaded_at: str, expected_rows: int, changed_after=None) -> int:
    """Append the staged changed rows to RAW.CRM.<TABLE>_CHANGES, except the versions it already
    holds (same id and updated_at): the lookback reads some rows again. Nothing is deleted or
    replaced: the table is the history of every change, and the stream reads it. Returns the
    versions appended. changed_after, the lower bound the rows were read with, lets Snowflake
    skip the older part of the change log."""
    staged = _staged_count(cursor, table, staging, brand, expected_rows)
    if not staged:
        return 0
    column_list = ", ".join(identifier(c.name) for c in columns)
    target = qualified(changes_table(table))
    since, params = ('and c."UPDATED_AT" > %s', (changed_after,)) if changed_after else ("", ())
    cursor.execute(
        f"insert into {target} ({column_list}, _BRAND, _LOADED_AT) "
        f"select {column_list}, %s, %s::timestamp_ntz from {qualified(staging)} as s "
        f"where not exists (select 1 from {target} as c "
        f'where c._BRAND = %s and c."ID" = s."ID" and c."UPDATED_AT" = s."UPDATED_AT" {since})',
        (brand, loaded_at, brand) + params,
    )
    return int(cursor.fetchone()[0])        # Snowflake answers an INSERT with the rows inserted


def ensure_figures_table(cursor, catalog: dict[str, list[str]] | None = None) -> None:
    """Create RAW.CRM._SOURCE_FIGURES unless the catalog shows it already exists."""
    if catalog is not None and SOURCE_FIGURES.upper() in catalog:
        return
    cursor.execute(
        f"create table if not exists {qualified(SOURCE_FIGURES)} "
        "(BRAND VARCHAR, TABLE_NAME VARCHAR, SOURCE_ROWS NUMBER(38, 0), UPDATED_AT_CHECKSUM NUMBER(38, 0), "
        "TAKEN_AT TIMESTAMP_NTZ)"
    )
    if catalog is not None:
        catalog[SOURCE_FIGURES.upper()] = ["BRAND", "TABLE_NAME", "SOURCE_ROWS", "UPDATED_AT_CHECKSUM", "TAKEN_AT"]


def record_figures(cursor, brand: str, figures: dict[str, TableFigures], taken_at: str) -> None:
    """Keep the source figures of the brand's tables, each read in the snapshot it was copied in.
    They replace the figures of the tables' previous load, in one transaction."""
    if not figures:
        return
    tables = list(figures)
    cursor.execute("begin")
    try:
        cursor.execute(
            f"delete from {qualified(SOURCE_FIGURES)} "
            f"where BRAND = %s and TABLE_NAME in ({', '.join(['%s'] * len(tables))})",
            (brand, *tables),
        )
        cursor.execute(
            f"insert into {qualified(SOURCE_FIGURES)} values "
            + ", ".join(["(%s, %s, %s, %s, %s::timestamp_ntz)"] * len(tables)),
            tuple(value for table in tables
                  for value in (brand, table, figures[table].rows, figures[table].updated_at_checksum, taken_at)),
        )
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise


def recorded_figures(cursor) -> dict[tuple[str, str], TableFigures]:
    """The source figures of every brand and incremental table, as of its last load."""
    cursor.execute(f"select BRAND, TABLE_NAME, SOURCE_ROWS, UPDATED_AT_CHECKSUM from {qualified(SOURCE_FIGURES)}")
    return {(brand, table): TableFigures(int(rows), int(checksum)) for brand, table, rows, checksum in cursor.fetchall()}


def bronze_figures(cursor, tables: tuple[SourceTable, ...]) -> dict[tuple[str, str], TableFigures]:
    """The same figures as source_postgres.table_figures, for every brand's rows in each
    RAW.CRM.<TABLE>, in one query. date_part(epoch_second) counts a TIMESTAMP_NTZ as UTC and a
    TIMESTAMP_TZ as its instant, as PostgreSQL's extract(epoch) does for timestamp and timestamptz.
    A brand with no rows in a table is not in the answer."""
    cursor.execute(
        " union all ".join(
            'select %s, _BRAND, count(*), coalesce(sum(date_part(epoch_second, "UPDATED_AT")), 0)::number(38, 0) '
            f"from {qualified(table.name)} group by _BRAND"
            for table in tables
        ),
        tuple(table.name for table in tables),
    )
    return {(brand, table): TableFigures(int(rows), int(checksum)) for table, brand, rows, checksum in cursor.fetchall()}


def publish_brand_slice(cursor, table: str, staging: str, columns: list[SourceColumn], brand: str,
                        loaded_at: str, expected_rows: int | None = None) -> int:
    """Replace the brand's rows in RAW.CRM.<TABLE> with the staged copy, in one transaction."""
    staged = _staged_count(cursor, table, staging, brand, expected_rows)
    cursor.execute(f"select count(*) from {qualified(table)} where _BRAND = %s", (brand,))
    live = cursor.fetchone()[0]
    if staged == 0 and live > 0:
        raise EmptyExtractError(
            f"{table} came back empty for {brand}, but bronze holds {live} rows for it. Nothing was replaced."
        )

    column_list = ", ".join(identifier(c.name) for c in columns)
    cursor.execute("begin")
    try:
        cursor.execute(f"delete from {qualified(table)} where _BRAND = %s", (brand,))
        cursor.execute(
            f"insert into {qualified(table)} ({column_list}, _BRAND, _LOADED_AT) "
            f"select {column_list}, %s, %s::timestamp_ntz from {qualified(staging)}",
            (brand, loaded_at),
        )
        cursor.execute("commit")
    except Exception:
        cursor.execute("rollback")
        raise
    return staged


@dataclasses.dataclass(frozen=True)
class TableLoad:
    """What load_table did to one table."""

    rows: int                               # rows published; for an incremental table, versions appended
    figures: TableFigures | None = None     # incremental tables: the source's figures in the load's snapshot


def load_table(source_conn, snowflake_conn, brand: str, table: SourceTable, loaded_at: str,
               chunk_size: int, load_method: str = "write_pandas",
               catalog: dict[str, list[str]] | None = None) -> TableLoad:
    """Copy one table. catalog is what RAW.CRM holds (bronze_schema.read_catalog) with the
    session already in RAW.CRM, as load_brand leaves it; without it, it is read here."""
    columns = source_postgres.table_columns(source_conn, table)
    staging = f"{table.name}__load_{brand}"
    cursor = snowflake_conn.cursor()
    try:
        if catalog is None:
            catalog = read_catalog(cursor)
            use_schema(cursor, catalog)
        ensure_bronze_table(cursor, catalog, table.name, columns)
        changed_after = already_loaded = None
        if table.incremental:
            # The merge task keeps RAW.CRM.<TABLE> current; the loader only appends changes.
            ensure_bronze_table(cursor, catalog, changes_table(table.name), columns)
            changed_after = watermark(cursor, table.name, brand)
            if changed_after is not None:
                changed_after -= INCREMENTAL_LOOKBACK
                already_loaded = loaded_versions(cursor, table.name, brand, changed_after)
        definitions = ", ".join(f"{identifier(c.name)} {snowflake_type(c)}" for c in columns)
        cursor.execute(f"create or replace transient table {qualified(staging)} ({definitions})")

        names = [c.name for c in columns]
        column_list = ", ".join(identifier(name) for name in names)
        id_at, updated_at_at = (names.index("id"), names.index("updated_at")) if already_loaded else (None, None)
        read = staged = 0
        for rows in source_postgres.iter_rows(source_conn, table, columns, chunk_size, changed_after):
            read += len(rows)
            if already_loaded:      # rows the lookback read again: bronze has them
                rows = [row for row in rows if (row[id_at], row[updated_at_at]) not in already_loaded]
            staged += len(rows)
            if not rows:
                continue
            if load_method == "insert" or len(rows) < WRITE_PANDAS_MIN_ROWS:
                # fakesnow's server cannot take write_pandas's file upload, and on Snowflake a
                # small chunk is quicker as INSERTs. executemany sends a multi-row INSERT per batch.
                placeholders = ", ".join(["%s"] * len(columns))
                values = to_insert_rows(rows, columns)
                for start in range(0, len(values), INSERT_BATCH_ROWS):
                    cursor.executemany(
                        f"insert into {qualified(staging)} ({column_list}) values ({placeholders})",
                        values[start:start + INSERT_BATCH_ROWS],
                    )
            else:
                write_pandas(
                    snowflake_conn,
                    to_frame(rows, columns),
                    table_name=staging.upper(),
                    database=BRONZE_DATABASE,
                    schema=BRONZE_SCHEMA,
                    quote_identifiers=True,
                    auto_create_table=False,
                    use_logical_type=True,
                )

        if table.incremental:
            loaded = append_changes(cursor, table.name, staging, columns, brand, loaded_at,
                                    expected_rows=staged, changed_after=changed_after)
            # Read in the snapshot the rows were just copied from, for reconcile()
            figures = source_postgres.table_figures(source_conn, table)
            log.info("%s: %s rows changed for %s since %s (%s read, %s already in bronze)", table.name, loaded,
                     brand, changed_after or "the first load", read, read - loaded)
            return TableLoad(loaded, figures)
        loaded = publish_brand_slice(cursor, table.name, staging, columns, brand, loaded_at, expected_rows=read)
        log.info("%s: loaded %s rows for %s", table.name, loaded, brand)
        return TableLoad(loaded)
    finally:
        cursor.execute(f"drop table if exists {qualified(staging)}")
        cursor.close()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def load_brand(snowflake_conn, brand: str, database: str,
               tables: tuple[SourceTable, ...] = SOURCE_TABLES, chunk_size: int = 50_000,
               load_method: str = "write_pandas") -> dict[str, int]:
    """Load the brand's source tables (all of them by default), all read in one snapshot, then
    record the source figures of the incremental ones. Returns the row count loaded per table."""
    loaded_at = _now()
    counts, figures = {}, {}
    cursor = snowflake_conn.cursor()
    try:
        catalog = read_catalog(cursor)
        use_schema(cursor, catalog)
        with source_postgres.connect(database, snapshot=True) as source_conn:
            for table in tables:
                load = load_table(source_conn, snowflake_conn, brand, table, loaded_at, chunk_size,
                                  load_method, catalog)
                counts[table.name] = load.rows
                if load.figures is not None:
                    figures[table.name] = load.figures
        if figures:
            ensure_figures_table(cursor, catalog)
            record_figures(cursor, brand, figures, loaded_at)
    finally:
        cursor.close()
    return counts


@dataclasses.dataclass
class Reconciliation:
    """One incremental table of one brand: the source figures recorded when it was loaded, and
    the figures of RAW.CRM.<TABLE> now."""

    table: str
    brand: str
    source: TableFigures
    bronze: TableFigures
    resynced: bool = False

    @property
    def matches(self) -> bool:
        return self.source == self.bronze

    @property
    def bronze_rows(self) -> int:
        return self.bronze.rows

    def __str__(self) -> str:
        text = f"{self.table} for {self.brand}: {self.source.rows} rows in the source, {self.bronze.rows} in bronze"
        if self.source.rows == self.bronze.rows and not self.matches:
            text += "; the updated_at checksums differ, so bronze holds an older version of at least one row"
        return text


NO_ROWS = TableFigures(0, 0)


def reconcile(snowflake_conn, brands, tables: tuple[SourceTable, ...] = INCREMENTAL_TABLES,
              resync: bool = False, chunk_size: int = 50_000,
              load_method: str = "write_pandas") -> list[Reconciliation]:
    """Compare every brand's rows in each merged table with the source. brands are
    config.Brand rows (key, source_database).

    The incremental load copies the rows whose updated_at moved on, so it never sees a row deleted
    in the source, nor a change that committed later than the lookback. The load records the
    table's row count and updated_at checksum in the snapshot it copied from (record_figures),
    and bronze must match them once the merges have run: a missing or extra row changes the count,
    a row bronze holds in an older version changes the checksum. Comparing with that snapshot
    rather than the source as it is now means rows the shop changed after the load are not a
    difference; the next load copies them. Two queries cover every brand and table, and nothing
    is read from the source.

    With resync, a table that differs is copied whole in a new snapshot, replacing the brand's rows
    in RAW.CRM.<TABLE> in one transaction (publish_brand_slice, as for the reference tables), and
    compared again with that snapshot's figures. Run it only after the merge tasks, when the stream
    holds nothing to merge.
    """
    cursor = snowflake_conn.cursor()
    results = []
    try:
        ensure_figures_table(cursor)
        recorded = recorded_figures(cursor)
        bronze = bronze_figures(cursor, tables)
        for brand in brands:
            with contextlib.ExitStack() as stack:
                source_conn = None          # opened only for a resync
                for table in tables:
                    source = recorded.get((brand.key, table.name))
                    if source is None:
                        raise PermanentError(
                            f"No source figures are recorded for {table.name} of {brand.key}: load the brand first."
                        )
                    result = Reconciliation(table.name, brand.key, source, bronze.get((brand.key, table.name), NO_ROWS))
                    if not result.matches and resync:
                        log.warning("%s. Copying it whole.", result)
                        if source_conn is None:
                            source_conn = stack.enter_context(
                                source_postgres.connect(brand.source_database, snapshot=True))
                        loaded_at = _now()
                        full_copy = dataclasses.replace(table, incremental=False)
                        load_table(source_conn, snowflake_conn, brand.key, full_copy, loaded_at, chunk_size, load_method)
                        source = source_postgres.table_figures(source_conn, table)
                        record_figures(cursor, brand.key, {table.name: source}, loaded_at)
                        after = bronze_figures(cursor, (table,)).get((brand.key, table.name), NO_ROWS)
                        result = Reconciliation(table.name, brand.key, source, after, resynced=True)
                    log.info("%s%s", result, "" if result.matches else ": DIFFERENT")
                    results.append(result)
    finally:
        cursor.close()
    return results


def mismatch_message(differences: list[Reconciliation], how_to_resync: str) -> str:
    """Why a run stops on a difference, and how to copy the tables whole once the source has been
    checked (how_to_resync: the orchestrator's own way to ask for it)."""
    return (
        "Bronze does not match the source:\n  " + "\n  ".join(map(str, differences))
        + f"\nIf the source really lost these rows, or a change was never copied, {how_to_resync} "
        "to copy these tables whole."
    )
