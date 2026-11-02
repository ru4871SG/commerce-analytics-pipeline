"""Shared retry policies and the PermanentError translation."""
from __future__ import annotations

from collections.abc import Callable

from dagster import Backoff, Failure, RetryPolicy

from crm_pipeline.errors import PermanentError

# Airflow's defaults: two retries, four minutes apart.
DEFAULT_RETRY = RetryPolicy(max_retries=2, delay=240, backoff=Backoff.LINEAR)
# trigger_crm_export answers 429 while an export is still running; give it time.
EXPORT_RETRY = RetryPolicy(max_retries=4, delay=180, backoff=Backoff.LINEAR)
# A chart that fails its query fails again; fix the bundle instead.
NO_RETRY = RetryPolicy(max_retries=0)


def permanent_failure(exc: PermanentError) -> Failure:
    """AirflowFailException's Dagster equivalent: fail now, do not use the retries."""
    return Failure(description=str(exc), allow_retries=False)


def call(fn: Callable, *args, **kwargs):
    """Run fn, turning PermanentError into a non-retryable Dagster Failure."""
    try:
        return fn(*args, **kwargs)
    except PermanentError as exc:
        raise permanent_failure(exc) from exc
