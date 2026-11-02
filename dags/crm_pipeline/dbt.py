"""Run the dbt project (dbt/) from Airflow.
"""
from __future__ import annotations

import logging
import os
import subprocess
from collections.abc import Callable

from airflow.sdk import Variable, get_current_context, task

from crm_pipeline.config import DBT_PROJECT_DIR
from crm_pipeline.dbt_env import build_args, dbt_environment

log = logging.getLogger(__name__)


def airflow_environment() -> dict[str, str]:
    """dbt's variables for snowflake_default. The target and the dev schema are the Airflow
    Variables crm_dbt_target and crm_dbt_dev_schema, which can be set in the UI."""
    environment = dbt_environment()
    environment["DBT_TARGET"] = Variable.get("crm_dbt_target", default="prod")
    environment["DBT_DEV_SCHEMA"] = Variable.get("crm_dbt_dev_schema", default="dbt_dev")
    return environment


def run_dbt(args: list[str], target_path: str | None, environment: dict[str, str]) -> None:
    """Run one dbt command in dbt/, its output in the task log. A failing command fails the task;
    a task that is stopped stops dbt too."""
    command = [os.environ.get("DBT_BIN", "dbt"), *args] + (["--target-path", target_path] if target_path else [])
    log.info("Running %s", " ".join(command))
    process = subprocess.Popen(
        command, cwd=DBT_PROJECT_DIR, env={**os.environ, **environment},
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    try:
        for line in process.stdout:
            log.info("%s", line.rstrip())
        process.wait()
    except BaseException:
        process.terminate()
        process.wait(timeout=60)
        raise
    if process.returncode:
        raise RuntimeError(f"dbt {args[0]} failed with exit code {process.returncode}")


def build(try_number: int, target_path: str, check_source_freshness: bool,
          environment: dict[str, str], run: Callable = run_dbt) -> None:
    """Install the dbt packages if needed, check freshness, then build, or on an Airflow retry
    resume the failed attempt with `dbt retry` when it left run results behind. The first try
    removes the previous run's results, so a retry never resumes yesterday's run. `dbt retry`
    takes no selection flags: it repeats the failed command with that command's own flags."""
    if not (DBT_PROJECT_DIR / "dbt_packages" / "dbt_utils").is_dir():
        run(["deps"], None, environment)
    results = DBT_PROJECT_DIR / target_path / "run_results.json"
    if try_number == 1:
        results.unlink(missing_ok=True)
    if check_source_freshness:
        # A stale bronze stops the task before anything is built
        run(["source", "freshness"], f"{target_path}/source_freshness", environment)
    if try_number > 1 and results.exists():
        run(["retry"], target_path, environment)
    else:
        run(build_args(environment["DBT_TARGET"]), target_path, environment)


def dbt_task(task_id: str, check_source_freshness: bool = False, **kwargs):
    """The task that builds silver and gold. kwargs are task arguments, e.g. outlets."""

    @task(task_id=task_id, **kwargs)
    def dbt_build() -> None:
        build(get_current_context()["ti"].try_number, f"target/airflow/{task_id}", check_source_freshness,
              airflow_environment())

    return dbt_build()
