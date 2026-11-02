#!/usr/bin/env python3
"""Run a DAG through the Airflow scheduler and follow it to the end
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from urllib.parse import quote

BASE_URL = "http://127.0.0.1:8080"
# The states a task instance is printed in; the last four are final
SHOWN = ("up_for_retry", "success", "skipped", "failed", "upstream_failed")
LOGIN_FAILED = 3


class Api:
    def __init__(self) -> None:
        self.token = ""

    def login(self) -> None:
        password = os.environ.get("AIRFLOW_PASSWORD")
        if not password:
            sys.exit("AIRFLOW_PASSWORD is not set: run scripts/local.sh install, which generates it in .env")
        try:
            self.token = self.send("POST", "/auth/token", {"username": "admin", "password": password})["access_token"]
        except urllib.error.HTTPError as error:
            if error.code == 401:
                print("Airflow refused admin's login with AIRFLOW_PASSWORD from .env.", file=sys.stderr)
                sys.exit(LOGIN_FAILED)
            raise

    def call(self, method: str, path: str, payload: dict | None = None) -> dict:
        if not self.token:
            self.login()
        try:
            return self.send(method, path, payload)
        except urllib.error.HTTPError as error:
            if error.code != 401:
                raise
            self.login()                  # the token expired
            return self.send(method, path, payload)

    def send(self, method: str, path: str, payload: dict | None = None) -> dict:
        request = urllib.request.Request(BASE_URL + path, method=method,
                                         data=None if payload is None else json.dumps(payload).encode())
        request.add_header("Content-Type", "application/json")
        if self.token:
            request.add_header("Authorization", f"Bearer {self.token}")
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read() or b"{}")


def parsed(api: Api, dag_id: str) -> bool:
    try:
        api.call("GET", f"/api/v2/dags/{dag_id}")
        return True
    except urllib.error.HTTPError as error:
        if error.code == 404:
            return False
        raise


def ready(api: Api, dag_ids: list[str], timeout: int = 300) -> int:
    """Wait until the API server answers and the DAG processor has parsed every DAG named."""
    deadline = time.monotonic() + timeout
    waiting = list(dag_ids)
    while True:
        try:
            waiting = [dag_id for dag_id in waiting if not parsed(api, dag_id)]
            if not waiting:
                return 0
        except urllib.error.HTTPError as error:
            if error.code < 500:
                raise
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass                          # the API server is still starting
        if time.monotonic() > deadline:
            print(f"Airflow has not parsed {', '.join(waiting)} after {timeout} s. See .local/airflow.log", file=sys.stderr)
            try:
                for error in api.call("GET", "/api/v2/importErrors")["import_errors"]:
                    print(f"  {error['filename']}:\n{error['stack_trace']}", file=sys.stderr)
            except (urllib.error.URLError, ConnectionError, TimeoutError):
                pass
            return 1
        time.sleep(2)


def task_instances(api: Api, dag_id: str, run_id: str) -> list[dict]:
    found: list[dict] = []
    while True:
        page = api.call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}/taskInstances"
                               f"?limit=100&offset={len(found)}")
        found += page["task_instances"]
        if not page["task_instances"] or len(found) >= page["total_entries"]:
            return found


def label(task: dict) -> str:
    return task["task_id"] if task.get("map_index", -1) == -1 else f"{task['task_id']}[{task['map_index']}]"


def run(api: Api, dag_id: str, run_id: str) -> int:
    """Switch the DAG on (a paused DAG's runs stay queued), trigger it, and follow the run."""
    api.call("PATCH", f"/api/v2/dags/{dag_id}", {"is_paused": False})
    api.call("POST", f"/api/v2/dags/{dag_id}/dagRuns",
             {"dag_run_id": run_id, "logical_date": None, "note": "Triggered by scripts/local.sh fullrun"})
    run_url = f"{BASE_URL}/dags/{dag_id}/runs/{quote(run_id, safe='')}"
    print(f"{dag_id}: run {run_id}, executed by the scheduler: {run_url}", flush=True)
    started, shown, said_queued = time.monotonic(), {}, False
    while True:
        dag_run = api.call("GET", f"/api/v2/dags/{dag_id}/dagRuns/{quote(run_id, safe='')}")
        run_state = dag_run["state"]
        if run_state == "queued" and not said_queued and time.monotonic() - started > 15:
            said_queued = True
            print("  still queued: max_active_runs is 1, so it starts when the run before it ends", flush=True)
        tasks = task_instances(api, dag_id, run_id)
        for task in sorted(tasks, key=lambda task: task.get("end_date") or ""):
            if task["state"] in SHOWN and shown.get(label(task)) != task["state"]:
                shown[label(task)] = task["state"]
                took = f"{task['duration']:.0f} s" if task.get("duration") is not None else ""
                print(f"  {label(task):<48} {task['state']:<16} {took}", flush=True)
        if run_state in ("success", "failed"):
            break
        time.sleep(3)

    minutes, seconds = divmod(round(time.monotonic() - started), 60)
    if run_state == "success":
        print(f"{dag_id}: run {run_id} succeeded in {minutes} min {seconds} s.", flush=True)
        return 0
    print(f"{dag_id}: run {run_id} failed. The logs of the failed tasks:", file=sys.stderr)
    for task in tasks:
        if task["state"] == "failed":
            print(f"  {run_url}/tasks/{quote(task['task_id'], safe='')}", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    waiting = commands.add_parser("ready", help="wait until the API answers and the DAGs are parsed")
    waiting.add_argument("dag_ids", nargs="+")
    triggered = commands.add_parser("run", help="switch the DAG on, trigger a run and follow it")
    triggered.add_argument("dag_id")
    triggered.add_argument("--run-id", default=f"fullrun__{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}")
    args = parser.parse_args(argv)
    api = Api()
    if args.command == "ready":
        return ready(api, args.dag_ids)
    return run(api, args.dag_id, args.run_id)


if __name__ == "__main__":
    sys.exit(main())
