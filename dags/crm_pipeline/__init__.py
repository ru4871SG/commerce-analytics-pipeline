"""Helpers shared by the Airflow DAGs and the Dagster job: extract from the brands' CRM
databases, load Snowflake's bronze layer, and run the dbt project that builds silver and gold."""
