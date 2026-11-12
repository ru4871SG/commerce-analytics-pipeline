"""Settings shared by the DAG and its helpers.

The brand list lives in one place, the dbt seed dbt/seeds/brands.csv, so the DAG extracts
exactly the brands the models know about.
"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT_DIR = Path(os.environ.get("CRM_DBT_PROJECT_DIR", REPO_ROOT / "dbt"))
BRANDS_SEED = DBT_PROJECT_DIR / "seeds" / "brands.csv"

# Connection IDs. Airflow resolves them from its metadata DB/secrets; Dagster, where Airflow is
# not installed, from the matching AIRFLOW_CONN_<ID> environment variables.
SOURCE_POSTGRES_CONN_ID = "crm_source_postgresql"  # the brands' CRM databases (one PostgreSQL server)
EXPORT_API_CONN_ID = "crm_export_api"              # optional: refreshes a brand's CRM export snapshot
SNOWFLAKE_CONN_ID = "snowflake_default"            # used by the bronze loader and by dbt
SLACK_CONN_ID = "slack_webhook"                    # optional: failure alerts
SOURCE_WRITER_CONN_ID = "crm_source_writer"        # demo only: the activity simulator writes the CRM
ECB_FX_CONN_ID = "ecb_fx_api"                      # ECB's daily exchange rates
SOURCE_CDC_CONN_ID = "crm_source_cdc"              # a replication role on the CRM server

# Demo mode: crm_daily_activity (Airflow) or the activity asset (Dagster) simulates a day of shop
# activity every morning, and the report runs when it has finished. Without it, the report runs
# on its own schedule.
SIMULATE_ACTIVITY = os.environ.get("CRM_SIMULATE_ACTIVITY", "false").strip().lower() == "true"
# The business runs on New York time: the day's activity is in at 07:00, the report follows.
TIMEZONE = "America/New_York"

# Where bronze lives in Snowflake. dbt reads the same values (models/bronze/_sources.yml).
BRONZE_DATABASE = os.environ.get("CRM_BRONZE_DATABASE", "RAW")
BRONZE_SCHEMA = "CRM"
# Reference data loaded beside the CRM by the side loads.
REF_SCHEMA = "REF"
# The ECB's rates up to the day the seed was written. The load asks the API for the days after it
FX_RATES_SEED = DBT_PROJECT_DIR / "seeds" / "fx_rates_ecb.csv"
# The database dbt builds silver and gold in, and the schema of the reporting tables (prod and
# emulator targets; dbt/macros/generate_schema_name.sql)
ANALYTICS_DATABASE = os.environ.get("CRM_ANALYTICS_DATABASE", "ANALYTICS")
GOLD_SCHEMA = "GOLD"

# local/superset/check_dashboard.py's exit code when Superset does not answer: the activation's
# dashboard check is skipped, not failed
SUPERSET_NOT_RUNNING = 99


@dataclass(frozen=True)
class Brand:
    key: str                  # lowercase brand key, also the _BRAND value in bronze
    name: str                 # name shown in dashboards
    source_database: str      # the brand's CRM database on the source server
    load_from_source: bool    # False: rows are already in RAW.CRM (e.g. dummy data)
    currency_code: str = "USD"


def load_brands(path: Path = BRANDS_SEED) -> list[Brand]:
    with open(path, newline="") as f:
        return [
            Brand(
                key=row["brand"].strip().lower(),
                name=row["brand_name"].strip(),
                source_database=row["source_database"].strip(),
                load_from_source=row["load_from_source"].strip().lower() == "true",
                currency_code=row.get("currency_code", "").strip() or "USD",
            )
            for row in csv.DictReader(f)
        ]


def brands_to_extract(path: Path = BRANDS_SEED) -> list[Brand]:
    return [brand for brand in load_brands(path) if brand.load_from_source]


@dataclass(frozen=True)
class SourceTable:
    """A CRM table copied to bronze. Columns in `exclude` are never copied; when `include` is
    set, only those columns are.

    incremental tables change every day. Only rows whose updated_at is newer than the last load
    are copied, appended to RAW.CRM.<TABLE>_CHANGES; a stream on that table and a task that
    merges it keep RAW.CRM.<TABLE> current (crm_pipeline/stream_task.py). The other tables are
    small reference data and are copied whole on every run.

    merge_after names the incremental tables this one's foreign keys point at: its merge task
    runs after theirs, so bronze never holds an order line whose order has not arrived yet.
    Tables with no parent in common merge in parallel."""

    name: str
    exclude: frozenset[str] = field(default_factory=frozenset)
    include: frozenset[str] | None = None
    incremental: bool = False
    merge_after: tuple[str, ...] = ()

    def keeps(self, column: str) -> bool:
        column = column.lower()
        if self.include is not None:
            return column in self.include
        return column not in self.exclude


# Every table the dbt models read. The source holds raw application data only: columns the old
# pipeline calculated (acquisition type, week strings, VAT-ready refund amounts) are derived in
# dbt. Customer emails are PII and never reach bronze; they are excluded here as a demonstration.
SOURCE_TABLES: tuple[SourceTable, ...] = (
    SourceTable("customers", exclude=frozenset({"email"}), incremental=True),
    SourceTable("product_costs"),
    SourceTable("processing_costs"),
    SourceTable("products"),
    SourceTable("product_variants"),
    SourceTable("orders", incremental=True, merge_after=("customers",)),
    SourceTable("order_items", incremental=True, merge_after=("orders",)),
    SourceTable("refund_payments", incremental=True, merge_after=("orders",)),
    # Daily ad spend per channel and campaign, imported into the CRM from the ad platforms
    SourceTable("marketing_spend", incremental=True),
)
INCREMENTAL_TABLES = tuple(table for table in SOURCE_TABLES if table.incremental)
REFERENCE_TABLES = tuple(table for table in SOURCE_TABLES if not table.incremental)

# The storefront clickstream on the lakehouse. Locally the lakehouse is a
# folder and the Spark engine runs in .venv-spark; with a databricks_default connection, the
# tables are Unity Catalog's and the Spark engine is the bundle's job.
DATABRICKS_CONN_ID = "databricks_default"          # optional: the Databricks workspace
LAKEHOUSE_CATALOG = os.environ.get("CRM_LAKEHOUSE_CATALOG", "local")
LAKEHOUSE_ROOT = os.environ.get("CRM_LAKEHOUSE_ROOT", str(REPO_ROOT / ".local" / "lakehouse"))
SPARK_PYTHON = os.environ.get("CRM_SPARK_PYTHON", str(REPO_ROOT / ".venv-spark" / "bin" / "python"))
# The local stand-in for a Databricks workspace (local/databricks_workspace.py), when it runs: the
# Spark engine is then the job the bundle deployed there, as on Databricks, not the source tree.
LOCAL_WORKSPACE = os.environ.get("CRM_LAKEHOUSE_LOCAL_WORKSPACE", "")
# What the in-process engine may use of the worker's memory, at its peak. A batch estimated above
# it goes to Spark (crm_lakehouse.planning.choose_engine).
PANDAS_BUDGET_BYTES = int(os.environ.get("CRM_LAKEHOUSE_PANDAS_BUDGET_MB", "1024")) * 1024 * 1024
# The warehouse schema the clickstream's gold is published to, beside RAW.CRM
WEB_SCHEMA = "WEB"

# How far before the last load's latest updated_at each incremental load reads again. A
# transaction sets updated_at when it starts (PostgreSQL's now()) but its rows appear only when it
# commits, so a change that commits after a load has read past its updated_at would otherwise
# never be copied. Rows read again that bronze already has are dropped before they are staged, or
# by the INSERT into the change log (bronze_loader.load_table, append_changes).
INCREMENTAL_LOOKBACK = timedelta(minutes=int(os.environ.get("CRM_INCREMENTAL_LOOKBACK_MINUTES", "60")))
