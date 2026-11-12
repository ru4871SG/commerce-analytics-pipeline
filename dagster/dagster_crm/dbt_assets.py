"""Every dbt model is a Dagster asset, generated from the manifest dagster-dbt reads.

`dbt source freshness` runs first in the same step when the job loads bronze, as in Airflow, on
every source; a stale bronze fails before `dbt build` can publish anything, and a stale
clickstream only warns. dbt itself orders silver before gold.
On the emulator the build excludes unit tests, as the Airflow task does: fakesnow cannot create
their temporary tables.
"""
from __future__ import annotations

import os
from pathlib import Path

from dagster import AssetKey, Failure
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from crm_pipeline import raw_tables
from crm_pipeline.config import DBT_PROJECT_DIR, brands_to_extract
from crm_pipeline.dbt_env import build_args, dbt_environment

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_BIN = os.environ.get("DBT_BIN", str(REPO_ROOT / ".venv-dbt" / "bin" / "dbt"))
DBT_TARGET_PATH = DBT_PROJECT_DIR / "target" / "dagster"

# dagster dev's DbtProject.prepare_if_dev() looks for `dbt` on PATH, not at DBT_BIN.
os.environ["PATH"] = f"{Path(DBT_BIN).parent}:{os.environ.get('PATH', '')}"

# dbt is a subprocess and inherits this process's environment.
os.environ.update(dbt_environment())

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROJECT_DIR,
    target_path=DBT_TARGET_PATH,
)


def _manifest_is_stale() -> bool:
    """Whether a dbt file has changed since manifest.json was written."""
    manifest = dbt_project.manifest_path
    if not manifest.exists():
        return True
    built = manifest.stat().st_mtime
    files = [DBT_PROJECT_DIR / "dbt_project.yml", DBT_PROJECT_DIR / "packages.yml", DBT_PROJECT_DIR / "profiles.yml"]
    for folder in ("models", "macros", "seeds", "tests"):
        files.extend(path for path in (DBT_PROJECT_DIR / folder).rglob("*") if path.is_file())
    return any(path.stat().st_mtime > built for path in files)


# Under dagster dev, prepare_if_dev() runs `dbt deps` and `dbt parse` in every process that loads
# these definitions, and the multiprocess executor starts one per step: one step's `dbt deps`
# reinstalls dbt_packages under another's `dbt parse`, and that step dies with "dbt_utils not
# found". scripts/local.sh parses before every Dagster command, so prepare only when a dbt file
# changed since, e.g. after editing a model and reloading the definitions in the UI.
if _manifest_is_stale():
    dbt_project.prepare_if_dev()

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir=DBT_PROJECT_DIR,
    dbt_executable=DBT_BIN,
)


# The asset group each dbt source is loaded in: its job's selection
SOURCE_GROUPS = {"crm": "bronze", "web": "lakehouse"}
# Tables of those schemas that a side load writes, not the schema's own job: RAW.CRM.ORDERS_CDC is
# the change data capture's
SIDE_LOAD_SOURCES = {("crm", "orders_cdc")}


class CrmDbtTranslator(DagsterDbtTranslator):
    """Bronze tables are the dbt sources; every model keeps its dbt name as its asset key."""

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props.get("resource_type")
        if resource_type == "source":
            # raw/crm/<table> from the bronze loader, raw/web/<table> from the lakehouse step
            return AssetKey(["raw", dbt_resource_props["source_name"], dbt_resource_props["name"]])
        # dagster-dbt's default key includes the folder (silver/stg_..., gold/fct_...); the
        # report and activation jobs refer to the dbt names, so use those.
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props.get("resource_type") == "source":
            # RAW.CRM is loaded by the report job, RAW.WEB by the clickstream job, and anything
            # else (RAW.REF, and the tables of SIDE_LOAD_SOURCES) by the side loads job
            if (dbt_resource_props["source_name"], dbt_resource_props["name"]) in SIDE_LOAD_SOURCES:
                return "crm_side_loads"
            return SOURCE_GROUPS.get(dbt_resource_props["source_name"], "crm_side_loads")
        return "dbt"


dbt_translator = CrmDbtTranslator()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    name="crm_dbt_assets",
    dagster_dbt_translator=dbt_translator,
    op_tags={"crm/report": "1"},
)
def crm_dbt_assets(context, dbt: DbtCliResource):
    # One target folder per command, reused by every run, as Airflow reuses target/airflow/<task_id>.
    # By default dagster-dbt writes a new folder (about 10 MB) per invocation and never removes it.
    # Freshness is only checked when this job loads bronze: brands that are not extracted (dummy
    # data) do not get fresher with time. It runs without the context: with it, dagster-dbt adds
    # `--select fqn:*`, which matches no source, so dbt checks nothing and passes. raise_on_error
    # is off so that the Failure below, without a retry, is what stops the step.
    # First of all, the side loads' tables: they are sources of dbt, and their job is not part of
    # this one, so a side load that has never run must leave an empty table, not a missing one.
    raw_tables.ensure_side_tables()
    if brands_to_extract():
        freshness = dbt.cli(["source", "freshness"], target_path=DBT_TARGET_PATH / "source_freshness",
                            raise_on_error=False).wait()
        if not freshness.is_successful():
            raise Failure(
                "dbt source freshness failed: bronze was not refreshed, so silver and gold are not rebuilt.",
                allow_retries=False,
            )

    args = build_args(dbt_environment()["DBT_TARGET"])
    yield from dbt.cli(args, context=context, target_path=DBT_TARGET_PATH / "build").stream()
