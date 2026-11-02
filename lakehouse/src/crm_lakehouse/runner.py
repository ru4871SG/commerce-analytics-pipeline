"""Start the Spark engine for a planned batch, from the orchestrator: on this computer, or as a
run of the Databricks job the bundle deployed.
"""
from __future__ import annotations

import logging
import os
import subprocess
from collections import deque
from collections.abc import Callable
from datetime import timedelta

from crm_lakehouse.layout import Lakehouse

log = logging.getLogger(__name__)


class SparkJobFailed(RuntimeError):
    """The Spark job ended without processing the batch."""


def run_local(lakehouse: Lakehouse, batch_id: str, python: str, orchestrator: str = "", run_id: str = "",
              echo: Callable[[str], None] = log.info) -> None:
    """Run spark_job in python (the interpreter of .venv-spark), its output in the task log. A
    failure raises with the end of that output; stopping the task stops Spark too."""
    command = [python, "-m", "crm_lakehouse.spark_job", "--catalog", lakehouse.catalog, "--root", lakehouse.root,
               "--batch-id", batch_id, "--orchestrator", orchestrator, "--run-id", run_id]
    echo(f"Running {' '.join(command)}")
    tail: deque[str] = deque(maxlen=30)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=os.environ.copy())
    try:
        for line in process.stdout:
            line = line.rstrip()
            echo(line)
            tail.append(line)
        process.wait()
    except BaseException:
        process.terminate()
        process.wait(timeout=60)
        raise
    if process.returncode:
        raise SparkJobFailed(f"The Spark job for batch {batch_id} failed with exit code {process.returncode}:\n"
                             + "\n".join(tail))


def run_on_databricks(workspace, job_name: str, batch_id: str, timeout: timedelta = timedelta(hours=3),
                      echo: Callable[[str], None] = log.info) -> str:
    """Trigger the bundle's job for the batch and wait for it. workspace is a
    databricks.sdk.WorkspaceClient, signed in as the orchestrator's service principal, which holds
    CAN_MANAGE_RUN on this job and nothing else. Returns the run's page."""
    from databricks.sdk.service.jobs import RunResultState

    jobs = list(workspace.jobs.list(name=job_name))
    if len(jobs) != 1:
        raise SparkJobFailed(f"Expected one Databricks job named {job_name!r}, found {len(jobs)}: deploy the bundle.")
    waiter = workspace.jobs.run_now(job_id=jobs[0].job_id, job_parameters={"batch_id": batch_id})
    echo(f"Started {job_name} (job {jobs[0].job_id}) run {waiter.response.run_id} for batch {batch_id}")
    run = waiter.result(timeout=timeout)
    state = run.state
    if state is None or state.result_state != RunResultState.SUCCESS:
        message = state.state_message if state else "no state"
        raise SparkJobFailed(f"{job_name} run {run.run_id} ended {state.result_state if state else None}: "
                             f"{message} ({run.run_page_url})")
    echo(f"{job_name} run {run.run_id} succeeded: {run.run_page_url}")
    return run.run_page_url
