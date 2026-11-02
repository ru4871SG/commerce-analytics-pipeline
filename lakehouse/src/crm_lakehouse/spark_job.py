"""The Spark job: process one planned batch.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

SETUP, HEALTHCHECK = "__setup__", "__healthcheck__"


def spark_session(app_name: str):
    """The job's Spark session. On Databricks, the session the job runs in (Spark Connect on
    serverless). Locally, open-source Spark with Delta Lake, bound to this computer only."""
    from pyspark.sql import SparkSession

    if os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        spark = SparkSession.builder.getOrCreate()
    else:
        from delta import configure_spark_with_delta_pip

        # Spark's Python workers must be this interpreter, not the first python3 on PATH
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        builder = (
            SparkSession.builder.appName(app_name)
            .master(os.environ.get("CRM_SPARK_MASTER", "local[*]"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.driver.memory", os.environ.get("CRM_SPARK_DRIVER_MEMORY", "4g"))
            .config("spark.sql.shuffle.partitions", os.environ.get("CRM_SPARK_SHUFFLE_PARTITIONS", "16"))
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.ui.enabled", "false")
        )
        if os.environ.get("CRM_SPARK_IVY_DIR"):
            builder = builder.config("spark.jars.ivy", os.environ["CRM_SPARK_IVY_DIR"])
        if os.environ.get("CRM_SPARK_WAREHOUSE_DIR"):
            # Otherwise Spark makes ./spark-warehouse wherever it starts; the local tables are paths
            builder = builder.config("spark.sql.warehouse.dir", os.environ["CRM_SPARK_WAREHOUSE_DIR"])
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
    # Business days are New York days of UTC instants; the conversion assumes a UTC session
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="web-events-spark", description=__doc__.split("\n\n")[0])
    parser.add_argument("--catalog", required=True)
    location = parser.add_mutually_exclusive_group(required=True)
    location.add_argument("--root", help="local runs: the folder that stands in for Unity Catalog")
    location.add_argument("--storage-root", help="Databricks: the external location of the shared tables")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--orchestrator", default="")
    parser.add_argument("--run-id", default="")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from crm_lakehouse import spark_engine
    from crm_lakehouse.layout import Lakehouse

    lakehouse = Lakehouse(args.catalog, root=args.root, storage_root=args.storage_root)
    if not args.batch_id:
        parser.error("--batch-id is empty: the orchestrator passes the batch it planned")
    spark = spark_session(f"web_events {args.batch_id}")
    if args.batch_id == SETUP:
        spark_engine.ensure_tables(spark, lakehouse)
        summary = {"setup": [lakehouse.name(table) for table in spark_engine.TABLES_TO_CREATE]}
    elif args.batch_id == HEALTHCHECK:
        summary = spark_engine.healthcheck(spark, lakehouse)
    else:
        summary = spark_engine.run_batch(spark, lakehouse, args.batch_id, args.orchestrator, args.run_id)
    print(json.dumps(summary, default=str), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
