#!/usr/bin/env python3
"""A local stand-in for a Databricks workspace's REST API, for the bundle's `local` target.
"""
from __future__ import annotations

import base64
import html
import json
import os
import re
import subprocess
import sys
import threading
import time
import zipfile
from email.parser import BytesParser
from email.policy import default
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

REPO = Path(__file__).resolve().parents[1]
HOME = REPO / ".local" / "databricks_workspace"
FILES = HOME / "files"
STATE = HOME / "state.json"
RUNS = HOME / "runs"
USER = os.environ.get("LOCAL_DATABRICKS_USER", "local-deployer@example.com")
SPARK_PYTHON = os.environ.get("CRM_SPARK_PYTHON", str(REPO / ".venv-spark" / "bin" / "python"))
LOCK = threading.RLock()
WORKSPACE_ID = "1000000000000001"


def load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"next_id": 1000, "objects": {"/": {"object_type": "DIRECTORY", "object_id": 1}},
            "jobs": {}, "runs": {}, "permissions": {}}


def save_state(state: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    temporary = STATE.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, indent=1, sort_keys=True))
    temporary.replace(STATE)


def new_id(state: dict) -> int:
    state["next_id"] += 1
    return state["next_id"]


def normal(path: str) -> str:
    path = "/" + unquote(path).strip("/")
    return path[len("/Workspace"):] or "/" if path.startswith("/Workspace/") or path == "/Workspace" else path


def mkdirs(state: dict, path: str) -> None:
    parts = normal(path).strip("/").split("/")
    for i in range(1, len(parts) + 1):
        current = "/" + "/".join(parts[:i])
        state["objects"].setdefault(current, {"object_type": "DIRECTORY", "object_id": new_id(state)})


def stored(path: str) -> Path:
    return FILES / normal(path).lstrip("/")


def write_file(state: dict, path: str, data: bytes) -> None:
    path = normal(path)
    mkdirs(state, path.rsplit("/", 1)[0] or "/")
    target = stored(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    old = state["objects"].get(path, {})
    state["objects"][path] = {"object_type": "FILE", "object_id": old.get("object_id") or new_id(state),
                              "size": len(data), "modified_at": int(time.time() * 1000)}


def info(path: str, entry: dict) -> dict:
    out = {"path": "/Workspace" + path if path != "/" else "/", "object_type": entry["object_type"],
           "object_id": entry["object_id"]}
    if entry["object_type"] == "FILE":
        out.update(size=entry.get("size", 0), modified_at=entry.get("modified_at", 0))
    return out


# --- running a deployed job ----------------------------------------------------------------

def _substitute(value: str, parameters: dict, run_id: int) -> str:
    value = re.sub(r"\{\{\s*job\.run_id\s*\}\}", str(run_id), value)
    return re.sub(r"\{\{\s*job\.parameters\.([A-Za-z0-9_]+)\s*\}\}", lambda m: str(parameters.get(m.group(1), "")), value)


def _wheel(settings: dict, task: dict) -> Path:
    environment = next(e for e in settings.get("environments", []) if e["environment_key"] == task["environment_key"])
    dependency = next(d for d in environment["spec"]["dependencies"] if d.endswith(".whl"))
    return stored(dependency)


def _entry_point(wheel: Path, package: str, name: str) -> str:
    with zipfile.ZipFile(wheel) as archive:
        for member in archive.namelist():
            if member.endswith(".dist-info/entry_points.txt"):
                for line in archive.read(member).decode().splitlines():
                    key, _, target = line.partition("=")
                    if key.strip() == name:
                        return target.strip()
    return f"{package}:{name}"


def execute(run_id: int) -> None:
    """Run every task of the run, one after another (the job has one), and record the outcome."""
    with LOCK:
        state = load_state()
        run = state["runs"][str(run_id)]
        settings = state["jobs"][str(run["job_id"])]["settings"]
        run["state"] = {"life_cycle_state": "RUNNING", "state_message": ""}
        save_state(state)
    log_path = RUNS / f"{run_id}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    result, message = "SUCCESS", ""
    with open(log_path, "w") as log:
        for task in settings.get("tasks", []):
            wheel_task = task.get("python_wheel_task")
            if wheel_task is None:
                result, message = "FAILED", f"task {task['task_key']}: only python_wheel_task runs here"
                break
            wheel = _wheel(settings, task)
            module, _, function = _entry_point(wheel, wheel_task["package_name"], wheel_task["entry_point"]).partition(":")
            arguments = [_substitute(a, run["job_parameters"], run_id) for a in wheel_task.get("parameters", [])]
            code = f"import sys\nfrom {module} import {function}\nsys.exit({function}(sys.argv[1:]))"
            environment = {**os.environ, "PYTHONPATH": str(wheel), "PYTHONNOUSERSITE": "1"}
            log.write(f"$ {SPARK_PYTHON} -c <{module}:{function}> {' '.join(arguments)}\n    (PYTHONPATH={wheel})\n")
            log.flush()
            process = subprocess.run([SPARK_PYTHON, "-c", code, *arguments], stdout=log, stderr=subprocess.STDOUT,
                                     env=environment, cwd=str(REPO))
            if process.returncode:
                result, message = "FAILED", f"task {task['task_key']} exited with {process.returncode}"
                break
    with LOCK:
        state = load_state()
        run = state["runs"][str(run_id)]
        run["state"] = {"life_cycle_state": "TERMINATED", "result_state": result, "state_message": message}
        run["end_time"] = int(time.time() * 1000)
        save_state(state)


# --- the status page -----------------------------------------------------------------------

def _when(milliseconds) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(milliseconds / 1000)) if milliseconds else ""


def _outcome(run: dict) -> str:
    status = run.get("state", {})
    if status.get("life_cycle_state") == "TERMINATED":
        return "succeeded" if status.get("result_state") == "SUCCESS" else "failed"
    return status.get("life_cycle_state", "").lower()


def status_page(state: dict) -> str:
    """What is deployed and what ran, as one page. The local workspace has no Databricks UI; this is
    what one would look at there: the job, who may run it, its runs and their logs."""
    e = html.escape
    jobs = sorted(state["jobs"].values(), key=lambda job: job["job_id"])
    names = {str(job["job_id"]): job["settings"].get("name", "") for job in jobs}
    job_rows = []
    for job in jobs:
        settings = job["settings"]
        wheels = [d.rsplit("/", 1)[-1] for env in settings.get("environments", [])
                  for d in env.get("spec", {}).get("dependencies", [])]
        grants = ", ".join(f"{g.get('service_principal_name') or g.get('user_name') or g.get('group_name')}: "
                           f"{g.get('permission_level')}" for g in state["permissions"].get(f"jobs/{job['job_id']}", []))
        job_rows.append(f"<tr><td>{job['job_id']}</td><td>{e(settings.get('name', ''))}</td><td>{e(', '.join(wheels))}</td>"
                        f"<td>{e(', '.join(p['name'] for p in settings.get('parameters', [])))}</td>"
                        f"<td>{settings.get('max_concurrent_runs', '')}</td><td>{e(grants)}</td>"
                        f"<td>{_when(job.get('created_time'))}</td></tr>")
    run_rows = []
    for run in sorted(state["runs"].values(), key=lambda run: run["run_id"], reverse=True):
        seconds = (run["end_time"] - run["start_time"]) / 1000 if run.get("end_time") else None
        outcome = _outcome(run)
        run_rows.append(f"<tr><td><a href=\"/runs/{run['run_id']}\">{run['run_id']}</a></td>"
                        f"<td>{e(names.get(str(run['job_id']), str(run['job_id'])))}</td>"
                        f"<td><code>{e(str(run['job_parameters'].get('batch_id', '')))}</code></td>"
                        f"<td class=\"{e(outcome)}\">{e(outcome)}</td><td>{_when(run.get('start_time'))}</td>"
                        f"<td>{'' if seconds is None else f'{seconds:,.0f} s'}</td>"
                        f"<td>{e(run.get('state', {}).get('state_message', ''))}</td></tr>")
    files = sorted((p, entry.get("size", 0)) for p, entry in state["objects"].items() if entry["object_type"] == "FILE")
    file_rows = [f"<tr><td><code>/Workspace{e(path)}</code></td><td>{size:,} B</td></tr>" for path, size in files]

    def table(headers, rows, empty):
        if not rows:
            return f"<p class=\"empty\">{empty}</p>"
        head = "".join(f"<th>{h}</th>" for h in headers)
        return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="10"><title>Local Databricks workspace</title>
<style>
:root {{ --bg: #ffffff; --fg: #1f2328; --muted: #59636e; --line: #d1d9e0; --ok: #1a7f37; --bad: #cf222e; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg: #0d1117; --fg: #e6edf3; --muted: #9198a1; --line: #3d444d; --ok: #3fb950; --bad: #f85149; }} }}
body {{ background: var(--bg); color: var(--fg); font: 14px/1.5 system-ui, sans-serif; margin: 24px auto; max-width: 1200px; padding: 0 16px; }}
h1 {{ font-size: 20px; margin-bottom: 4px; }} h2 {{ font-size: 16px; margin-top: 28px; }}
p {{ color: var(--muted); margin: 4px 0; }} table {{ border-collapse: collapse; width: 100%; overflow-x: auto; display: block; }}
th, td {{ border-bottom: 1px solid var(--line); padding: 6px 10px; text-align: left; vertical-align: top; }}
th {{ color: var(--muted); font-weight: 600; }} a {{ color: inherit; }}
.succeeded {{ color: var(--ok); font-weight: 600; }} .failed {{ color: var(--bad); font-weight: 600; }} .empty {{ font-style: italic; }}
</style></head><body>
<h1>Local Databricks workspace</h1>
<p>A stand-in for a workspace's Jobs and Workspace APIs (local/databricks_workspace.py): the Databricks CLI deploys the
bundle here, and a run runs the deployed wheel with local Spark. Not a Databricks UI; refreshes every 10 seconds.</p>
<p>Signed in as <code>{e(USER)}</code>. State in <code>{e(str(HOME))}</code>.</p>
<h2>Jobs</h2>
{table(["Id", "Name", "Wheel", "Parameters", "Concurrent runs", "Permissions", "Deployed"], job_rows,
       "Nothing deployed yet: scripts/local.sh lakehouse release -t local, then deploy.")}
<h2>Runs</h2>
{table(["Run", "Job", "batch_id", "Outcome", "Started", "Took", "Message"], run_rows, "No run yet.")}
<h2>Files</h2>
{table(["Path", "Size"], file_rows, "No file uploaded yet.")}
</body></html>
"""


# --- the API -------------------------------------------------------------------------------

def parse_body(content_type: str, body: bytes) -> dict:
    """A JSON body, or a multipart form (workspace import sends the file that way): field -> value,
    with the uploaded file's bytes base64-encoded under "content", as the JSON form carries them."""
    if not body:
        return {}
    if content_type.startswith("multipart/form-data"):
        message = BytesParser(policy=default).parsebytes(
            f"Content-Type: {content_type}\r\n\r\n".encode() + body)
        fields = {}
        for part in message.iter_parts():
            name = part.get_param("name", header="content-disposition")
            data = part.get_payload(decode=True) or b""
            fields[name] = base64.b64encode(data).decode() if name == "content" else data.decode()
        if "overwrite" in fields:
            fields["overwrite"] = fields["overwrite"] == "true"
        return fields
    try:
        parsed = json.loads(body)
    except ValueError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class Workspace(BaseHTTPRequestHandler):
    server_version = "LocalDatabricksWorkspace/1"

    def reply(self, code: int, payload=None, raw: bytes | None = None, content_type: str | None = None) -> None:
        data = raw if raw is not None else json.dumps(payload if payload is not None else {}).encode()
        self.send_response(code)
        self.send_header("Content-Type", content_type
                         or ("application/octet-stream" if raw is not None else "application/json"))
        self.send_header("Content-Length", str(len(data)))
        # Real workspaces name themselves in every response; the CLI builds links from it
        self.send_header("X-Databricks-Org-Id", WORKSPACE_ID)
        self.end_headers()
        self.wfile.write(data)

    def missing(self, what: str) -> None:
        self.reply(404, {"error_code": "RESOURCE_DOES_NOT_EXIST", "message": f"{what} does not exist"})

    def handle_any(self) -> None:
        url = urlparse(self.path)
        query = {key: values[-1] for key, values in parse_qs(url.query).items()}
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        payload = parse_body(self.headers.get("Content-Type", ""), body)
        route = f"{self.command} {re.sub(r'/api/2\.[0-9]/', '/api/', url.path)}"
        with LOCK:
            state = load_state()
            handled = self.route(route, url.path, query, payload, body, state)
            if handled is not None:
                save_state(state)
        if handled is None:
            sys.stderr.write(f"not implemented: {self.command} {self.path}\n")
            self.reply(404, {"error_code": "ENDPOINT_NOT_FOUND", "message": f"{route} is not implemented locally"})

    def route(self, route, path, query, payload, body, state):
        objects = state["objects"]
        if route == "GET /":
            return self.reply(200, raw=status_page(state).encode(), content_type="text/html; charset=utf-8") or True
        match = re.fullmatch(r"GET /runs/([0-9]+)", route)
        if match:
            log = RUNS / f"{match.group(1)}.log"
            if str(match.group(1)) not in state["runs"]:
                return self.missing(f"run {match.group(1)}") or True
            text = log.read_text() if log.exists() else "(no output yet)"
            return self.reply(200, raw=text.encode(), content_type="text/plain; charset=utf-8") or True
        if route == "GET /api/preview/scim/v2/Me":
            return self.reply(200, {"id": "1", "userName": USER, "displayName": USER, "active": True,
                                    "groups": [{"display": "users"}]}) or True
        if route == "POST /telemetry-ext":
            return self.reply(200, {}) or True
        if route == "GET /api/workspace/get-status":
            entry = objects.get(normal(query.get("path", "/")))
            return (self.reply(200, info(normal(query["path"]), entry)) if entry else self.missing(query["path"])) or True
        if route == "POST /api/workspace/mkdirs":
            mkdirs(state, payload["path"])
            return self.reply(200, {}) or True
        if route == "GET /api/workspace/list":
            base = normal(query.get("path", "/"))
            if base not in objects:
                return self.missing(base) or True
            children = [info(p, e) for p, e in objects.items()
                        if p != base and p.rsplit("/", 1)[0] == (base if base != "/" else "")]
            return self.reply(200, {"objects": sorted(children, key=lambda o: o["path"])}) or True
        if route == "POST /api/workspace/delete":
            target = normal(payload["path"])
            for p in [p for p in objects if p == target or (payload.get("recursive") and p.startswith(target + "/"))]:
                objects.pop(p)
                if stored(p).is_file():
                    stored(p).unlink()
            return self.reply(200, {}) or True
        if route == "POST /api/workspace/import":
            write_file(state, payload["path"], base64.b64decode(payload.get("content", "")))
            return self.reply(200, {}) or True
        if route == "GET /api/workspace/export":
            target = normal(query["path"])
            if objects.get(target, {}).get("object_type") != "FILE":
                return self.missing(target) or True
            if query.get("direct_download") == "true":
                return self.reply(200, raw=stored(target).read_bytes()) or True
            return self.reply(200, {"content": base64.b64encode(stored(target).read_bytes()).decode(),
                                    "file_type": "file"}) or True
        if path.startswith("/api/2.0/workspace-files/import-file/") and self.command == "POST":
            write_file(state, "/" + path.split("/import-file/", 1)[1], body)
            return self.reply(200, {}) or True
        if path.startswith("/api/2.0/workspace-files/") and self.command == "GET":
            target = normal("/" + path.split("/workspace-files/", 1)[1])
            if objects.get(target, {}).get("object_type") != "FILE":
                return self.missing(target) or True
            return self.reply(200, raw=stored(target).read_bytes()) or True

        if route == "POST /api/jobs/create":
            job_id = new_id(state)
            state["jobs"][str(job_id)] = {"job_id": job_id, "creator_user_name": USER, "run_as_user_name": USER,
                                          "created_time": int(time.time() * 1000), "settings": payload}
            return self.reply(200, {"job_id": job_id}) or True
        if route in ("POST /api/jobs/reset", "POST /api/jobs/update"):
            job = state["jobs"].get(str(payload["job_id"]))
            if job is None:
                return self.missing(f"job {payload['job_id']}") or True
            job["settings"] = payload["new_settings"] if route.endswith("reset") else {**job["settings"], **payload.get("new_settings", {})}
            return self.reply(200, {}) or True
        if route == "GET /api/jobs/get":
            job = state["jobs"].get(str(query["job_id"]))
            return (self.reply(200, job) if job else self.missing(f"job {query['job_id']}")) or True
        if route == "POST /api/jobs/delete":
            state["jobs"].pop(str(payload["job_id"]), None)
            return self.reply(200, {}) or True
        if route == "GET /api/jobs/list":
            jobs = [job for job in state["jobs"].values()
                    if "name" not in query or job["settings"].get("name") == query["name"]]
            return self.reply(200, {"jobs": jobs, "has_more": False}) or True
        if route == "POST /api/jobs/run-now":
            job = state["jobs"].get(str(payload["job_id"]))
            if job is None:
                return self.missing(f"job {payload['job_id']}") or True
            run_id = new_id(state)
            defaults = {p["name"]: p.get("default", "") for p in job["settings"].get("parameters", [])}
            state["runs"][str(run_id)] = {
                "run_id": run_id, "job_id": job["job_id"], "number_in_job": run_id,
                "job_parameters": {**defaults, **payload.get("job_parameters", {})},
                "start_time": int(time.time() * 1000), "run_page_url": f"http://127.0.0.1:{self.server.server_port}/runs/{run_id}",
                "state": {"life_cycle_state": "PENDING", "state_message": ""},
            }
            threading.Thread(target=execute, args=(run_id,), daemon=True).start()
            return self.reply(200, {"run_id": run_id, "number_in_job": run_id}) or True
        if route == "GET /api/jobs/runs/get":
            run = state["runs"].get(str(query["run_id"]))
            if run is None:
                return self.missing(f"run {query['run_id']}") or True
            parameters = [{"name": k, "value": v} for k, v in run["job_parameters"].items()]
            return self.reply(200, {**run, "job_parameters": parameters,
                                    "tasks": [{"task_key": "process_batch", "run_id": run["run_id"], "state": run["state"]}]}) or True
        if route == "GET /api/jobs/runs/get-output":
            log = RUNS / f"{query['run_id']}.log"
            return self.reply(200, {"logs": log.read_text() if log.exists() else ""}) or True

        match = re.fullmatch(r"(GET|PUT|PATCH) /api/permissions/([a-z-]+)/(.+)", route)
        if match:
            method, kind, object_id = match.groups()
            key = f"{kind}/{object_id}"
            if method in ("PUT", "PATCH"):
                acl = payload.get("access_control_list", [])
                state["permissions"][key] = acl if method == "PUT" else state["permissions"].get(key, []) + acl
            # Read back as the API reports it: each principal with all_permissions
            listed = [{**{k: v for k, v in entry.items() if k != "permission_level"},
                       "all_permissions": [{"permission_level": entry["permission_level"], "inherited": False}]}
                      for entry in state["permissions"].get(key, [])]
            return self.reply(200, {"object_id": f"/{kind}/{object_id}", "object_type": kind.rstrip("s"),
                                    "access_control_list": listed}) or True
        return None

    do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = handle_any

    def log_message(self, *args):
        pass


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8770
    HOME.mkdir(parents=True, exist_ok=True)
    server = ThreadingHTTPServer(("127.0.0.1", port), Workspace)
    print(f"READY local Databricks workspace on http://127.0.0.1:{port} (state in {HOME})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
