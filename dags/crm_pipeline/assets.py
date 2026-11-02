"""Airflow assets the DAGs share. An asset update is what starts a DAG scheduled on it."""
from airflow.sdk import Asset

# A day of shop activity is in the CRM (crm_daily_activity, demo only). The report DAG runs on it.
ACTIVITY = Asset(name="crm_source_daily_activity")

# Gold has been rebuilt and tested. Downstream DAGs (an export, a dashboard cache refresh) can be
# scheduled on it.
GOLD = Asset(name="crm_executive_report_gold")
