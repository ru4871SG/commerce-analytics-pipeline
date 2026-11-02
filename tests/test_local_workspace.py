"""The local stand-in for a Databricks workspace (local/databricks_workspace.py): the parts that
must read the Databricks CLI's requests as Databricks does. The full deploy and run are exercised
by `scripts/local.sh lakehouse release|deploy|verify -t local` with the real CLI."""
from __future__ import annotations

import base64
import importlib.util
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("databricks_workspace", REPO / "local" / "databricks_workspace.py")
workspace = importlib.util.module_from_spec(spec)
spec.loader.exec_module(workspace)


def test_a_workspace_import_sent_as_a_multipart_form_is_read_like_the_json_one():
    boundary = "b0undary"
    body = (
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"path\"\r\n\r\n/Workspace/Users/x/state/deploy.lock\r\n"
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"overwrite\"\r\n\r\ntrue\r\n"
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"content\"; filename=\"blob\"\r\n"
        f"Content-Type: application/octet-stream\r\n\r\nLOCK\r\n--{boundary}--\r\n"
    ).encode()
    fields = workspace.parse_body(f"multipart/form-data; boundary={boundary}", body)
    assert fields["path"] == "/Workspace/Users/x/state/deploy.lock" and fields["overwrite"] is True
    assert base64.b64decode(fields["content"]) == b"LOCK"
    assert workspace.parse_body("application/json", json.dumps({"path": "/a"}).encode()) == {"path": "/a"}
    assert workspace.parse_body("application/json", b"") == {}


def test_paths_with_and_without_the_workspace_prefix_are_the_same_object():
    assert workspace.normal("/Workspace/Users/x/a.whl") == workspace.normal("/Users/x/a.whl") == "/Users/x/a.whl"
    assert workspace.normal("/Workspace") == "/"


def test_a_run_fills_in_the_job_parameters_and_its_own_id():
    value = workspace._substitute("--batch-id={{job.parameters.batch_id}} {{ job.run_id }} {{job.run_id}}",
                                  {"batch_id": "b-1"}, 77)
    assert value == "--batch-id=b-1 77 77"


def test_the_status_page_shows_the_deployed_job_its_permissions_and_its_runs():
    state = {
        "objects": {"/Users/x/.bundle/a.whl": {"object_type": "FILE", "object_id": 2, "size": 30_000}},
        "jobs": {"10": {"job_id": 10, "created_time": 0, "settings": {
            "name": "web_events_spark", "max_concurrent_runs": 1, "parameters": [{"name": "batch_id"}],
            "environments": [{"spec": {"dependencies": ["/Workspace/Users/x/.bundle/a.whl"]}}]}}},
        "runs": {"11": {"run_id": 11, "job_id": 10, "job_parameters": {"batch_id": "<b1>"}, "start_time": 1000,
                        "end_time": 84_000, "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"}}},
        "permissions": {"jobs/10": [{"service_principal_name": "orchestrator", "permission_level": "CAN_MANAGE_RUN"}]},
    }
    page = workspace.status_page(state)
    assert "web_events_spark" in page and "a.whl" in page and "orchestrator: CAN_MANAGE_RUN" in page
    assert '<a href="/runs/11">11</a>' in page and "succeeded" in page and "83 s" in page
    assert "&lt;b1&gt;" in page and "<b1>" not in page                  # what a run was given is escaped
