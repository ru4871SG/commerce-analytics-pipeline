"""Bronze: ingest each brand, merge the change streams, then reconcile with the source.
"""
from __future__ import annotations

from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetKey,
    AssetOut,
    AssetSpec,
    Failure,
    MaterializeResult,
    Out,
    Output,
    graph_multi_asset,
    multi_asset,
    op,
)

from crm_pipeline import bronze_loader, export_api, source_postgres, stream_task
from crm_pipeline.config import (
    INCREMENTAL_TABLES,
    REFERENCE_TABLES,
    SOURCE_TABLES,
    brands_to_extract,
)
from crm_pipeline.stream_task import changes_table

from dagster_crm.resources import SnowflakeWarehouse
from dagster_crm.retries import DEFAULT_RETRY, EXPORT_RETRY, call

BRANDS = brands_to_extract()
BRONZE = "bronze"


def raw_key(name: str) -> AssetKey:
    return AssetKey(["raw", "crm", name])


def _prepare_op(brand):
    @op(name=f"prepare_{brand.key}", retry_policy=EXPORT_RETRY)
    def prepare(context) -> str:
        call(export_api.trigger_crm_export, brand.key)
        call(source_postgres.check_source_tables, brand.source_database, SOURCE_TABLES)
        context.log.info("%s: export is current and every source table exists.", brand.key)
        return brand.key

    return prepare


def _load_op(brand):
    @op(name=f"load_{brand.key}", retry_policy=DEFAULT_RETRY)
    def load(context, warehouse: SnowflakeWarehouse, prepared: str) -> dict[str, int]:
        with warehouse.session() as connection:
            counts = call(
                bronze_loader.load_brand,
                connection,
                brand.key,
                brand.source_database,
                SOURCE_TABLES,
                load_method=warehouse.load_method(),
            )
        context.log.info("%s: %s", brand.key, ", ".join(f"{table} {rows}" for table, rows in counts.items()))
        return counts

    return load


PREPARE_OPS = {brand.key: _prepare_op(brand) for brand in BRANDS}
LOAD_OPS = {brand.key: _load_op(brand) for brand in BRANDS}


@op(
    name="collect_loads",
    out={
        **{table.name: Out() for table in REFERENCE_TABLES},
        **{f"{table.name}_changes": Out() for table in INCREMENTAL_TABLES},
    },
)
def collect_loads(results: list[dict[str, int]]):
    totals: dict[str, int] = {}
    for result in results:
        for table, rows in result.items():
            totals[table] = totals.get(table, 0) + rows
    return tuple(
        Output(totals.get(table.name, 0), output_name=table.name, metadata={"rows": totals.get(table.name, 0)})
        for table in REFERENCE_TABLES
    ) + tuple(
        Output(
            totals.get(table.name, 0),
            output_name=f"{table.name}_changes",
            metadata={"changed_rows": totals.get(table.name, 0)},
        )
        for table in INCREMENTAL_TABLES
    )


@graph_multi_asset(
    name="bronze_ingest",
    group_name=BRONZE,
    can_subset=False,
    outs={
        **{
            table.name: AssetOut(
                key=raw_key(table.name),
                group_name=BRONZE,
                description=f"The {table.name} reference table, all brands, copied whole.",
            )
            for table in REFERENCE_TABLES
        },
        **{
            f"{table.name}_changes": AssetOut(
                key=raw_key(changes_table(table.name)),
                group_name=BRONZE,
                description=f"Append-only changed rows for {table.name}.",
            )
            for table in INCREMENTAL_TABLES
        },
    },
)
def bronze_ingest():
    loads = [LOAD_OPS[brand.key](PREPARE_OPS[brand.key]()) for brand in BRANDS]
    return collect_loads(loads)


def in_merge_order(tables):
    """The incremental tables with every table after the ones its foreign keys point at."""
    ordered, merged, pending = [], set(), list(tables)
    while pending:
        ready = [table for table in pending if set(table.merge_after) <= merged]
        if not ready:
            raise ValueError(f"merge_after has a cycle among {[table.name for table in pending]}")
        for table in ready:
            ordered.append(table)
            merged.add(table.name)
            pending.remove(table)
    return ordered


MERGE_ORDER = in_merge_order(INCREMENTAL_TABLES)


def _merge_spec(table) -> AssetSpec:
    parents = ", ".join(table.merge_after) or "no parent table"
    return AssetSpec(
        key=raw_key(table.name),
        deps=[raw_key(changes_table(table.name))] + [raw_key(parent) for parent in table.merge_after],
        group_name=BRONZE,
        description=f"MERGE_{table.name.upper()}: carry the stream into bronze after {parents}.",
    )


def _reconcile_spec(table) -> AssetCheckSpec:
    return AssetCheckSpec(
        name=f"reconcile_{table.name}",
        asset=raw_key(table.name),
        blocking=True,
        description=f"Compare {table.name} with the source figures recorded at its load, per brand.",
    )


@multi_asset(
    name="bronze_merge",
    specs=[_merge_spec(table) for table in MERGE_ORDER],
    check_specs=[_reconcile_spec(table) for table in MERGE_ORDER],
    can_subset=False,
    retry_policy=DEFAULT_RETRY,
)
def bronze_merge(context, warehouse: SnowflakeWarehouse):
    """The five merge tasks, parents first, then the reconciliation of every merged table: one
    step, so the five merges and five checks do not each start a process."""
    # A run tag rather than run config, so a resync is launched the same way from the CLI and the
    # Launchpad, and the run list shows which runs resynced.
    resync = context.run.tags.get("crm/resync_bronze") == "true"
    with warehouse.session() as connection:
        cursor = connection.cursor()
        catalog = stream_task.ensure_metadata(cursor)
        for table in MERGE_ORDER:
            changes = stream_task.read_stream(cursor, table.name, catalog)
            stream_task.log_stream(changes)
            run = call(stream_task.run_merge_task, cursor, table.name, context.run.run_id, "dagster", changes)
            yield MaterializeResult(
                asset_key=raw_key(table.name),
                metadata={
                    "state": run.state,
                    "inserted": run.inserted,
                    "updated": run.updated,
                    "stream_rows": changes.rows,
                },
            )
        results = call(
            bronze_loader.reconcile,
            connection,
            BRANDS,
            tables=tuple(MERGE_ORDER),
            resync=resync,
            load_method=warehouse.load_method(),
        )

    for table in MERGE_ORDER:
        mine = [result for result in results if result.table == table.name]
        yield AssetCheckResult(
            asset_key=raw_key(table.name),
            check_name=f"reconcile_{table.name}",
            passed=all(result.matches for result in mine),
            metadata={"brands": len(mine), **{result.brand: result.bronze_rows for result in mine}},
        )
    differences = [result for result in results if not result.matches]
    if differences:
        # Retrying cannot make bronze match: fail now, as Airflow's reconcile_bronze does
        raise Failure(
            bronze_loader.mismatch_message(differences, "run the report with the crm/resync_bronze=true tag"),
            allow_retries=False,
        )


BRONZE_ASSETS = [bronze_ingest, bronze_merge]
