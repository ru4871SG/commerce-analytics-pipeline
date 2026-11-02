"""Messages to Slack: failure alerts and the executive digest. Without a `slack_webhook`
connection a message is only logged, so the DAGs also run with no Slack set up.

Under Airflow the message goes through SlackWebhookHook. Without Airflow (the Dagster
environment) it is posted to the webhook URL, which is the connection's password.
"""
from __future__ import annotations

import logging

from crm_pipeline import connections
from crm_pipeline.config import SLACK_CONN_ID

log = logging.getLogger(__name__)


def post_to_slack(text: str) -> bool:
    """Send text to the Slack webhook. Returns False, having logged the text, when there is none."""
    try:
        conn = connections.get_connection(SLACK_CONN_ID)
    except Exception:
        log.warning("No %s connection: not sending this message to Slack.\n%s", SLACK_CONN_ID, text)
        return False

    if connections.airflow_installed():
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID).send(text=text)
        return True

    import requests

    response = requests.post(conn.password, json={"text": text}, timeout=30)
    response.raise_for_status()
    return True


def failure_text(job: str, run_id: str, error, url: str = "") -> str:
    """A failure alert that does not need an orchestrator's task context."""
    return (
        f":red_circle: *{job}* failed\n"
        f"Run: {run_id}\n"
        f"Error: {error}\n"
        f"Log: {url}"
    )


def failure_message(context) -> str:
    ti = context["ti"]
    return (
        f":red_circle: *{ti.dag_id} › {ti.task_id}* failed on try {ti.try_number}\n"
        f"Run: {context['run_id']}\n"
        f"Error: {context.get('exception')}\n"
        f"Log: {getattr(ti, 'log_url', '')}"
    )


def notify_failure(context) -> None:
    """on_failure_callback: called once a task has used up its retries."""
    post_to_slack(failure_message(context))
