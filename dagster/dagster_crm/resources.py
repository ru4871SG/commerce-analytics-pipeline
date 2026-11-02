"""Thin Dagster resources over the orchestrator-neutral helpers in crm_pipeline."""
from __future__ import annotations

from dagster import ConfigurableResource

from crm_pipeline import bronze_loader
from crm_pipeline.config import SNOWFLAKE_CONN_ID


class SnowflakeWarehouse(ConfigurableResource):
    """The warehouse the bronze loader and the digest read and write."""

    conn_id: str = SNOWFLAKE_CONN_ID

    def connection(self):
        return bronze_loader.snowflake_connection(self.conn_id)

    def session(self):
        """A connection for a `with` block, closed when it ends."""
        return bronze_loader.snowflake_session(self.conn_id)

    def load_method(self) -> str:
        return bronze_loader.load_method(self.conn_id)
