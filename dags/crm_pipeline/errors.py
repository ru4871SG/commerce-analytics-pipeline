from __future__ import annotations

try:
    from airflow.sdk.exceptions import AirflowFailException as _Base
except ImportError:  # no Airflow: the Dagster environment
    _Base = Exception


class PermanentError(_Base):
    """The input or configuration is wrong; running the same task again cannot help."""
