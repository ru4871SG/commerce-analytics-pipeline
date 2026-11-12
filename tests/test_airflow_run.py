"""the script fullrun triggers and follows DAG runs with. a run goes on in the
scheduler while Airflow's API is too busy to answer, so the script waits for the API to answer
again instead of ending on the first request that times out. The real Airflow is exercised by
`scripts/local.sh fullrun`."""
from __future__ import annotations

import importlib.util
import json
import re
import threading
import time
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("airflow_run", REPO / "local" / "airflow_run.py")
airflow_run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(airflow_run)

PASSWORD = "pw-never-printed"
RUN = "/api/v2/dags/d/dagRuns/r"
TASKS = {"task_instances": [{"task_id": "t", "state": "success", "duration": 1.0, "end_date": "x", "map_index": -1}],
         "total_entries": 1}


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    monkeypatch.setenv("AIRFLOW_PASSWORD", PASSWORD)
    monkeypatch.setattr(airflow_run, "POLL_INTERVAL", 0)


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("http://127.0.0.1:8080/x", code, "error", {}, None)


class Scripted:
    """Api.send replaced: answer(method, path, nth) returns the answer or raises; every call is kept."""

    def __init__(self, answer):
        self.answer, self.calls = answer, []

    def __call__(self, method, path, payload=None):     # set on the class, but not bound: no self of Api's
        self.calls.append((method, path))
        return self.answer(method, path, sum(1 for call in self.calls if call == (method, path)))

    def count(self, method, path):
        return self.calls.count((method, path))


def airflow(method, path, nth):
    """A run that is running at the first look and has succeeded at the second."""
    if path == "/auth/token":
        return {"access_token": "token"}
    if path.startswith(RUN + "/taskInstances"):
        return TASKS
    if path == RUN:
        return {"state": "running" if nth == 1 else "success"}
    return {}


def test_a_poll_that_times_out_on_a_real_socket_is_tried_again_and_the_run_is_followed_to_its_end(
        monkeypatch, capsys):
    polls = []

    class Handler(BaseHTTPRequestHandler):
        def answer(self):
            path = self.path
            if self.command == "GET" and path == RUN:
                polls.append(path)
                if len(polls) == 2:
                    time.sleep(1.0)           # longer than the request's timeout: the client gives up
            body = json.dumps(airflow(self.command, path, len(polls) - (len(polls) > 2))).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass                          # the client has given up on this one

        do_GET = do_POST = do_PATCH = answer

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        monkeypatch.setattr(airflow_run, "BASE_URL", f"http://127.0.0.1:{server.server_address[1]}")
        monkeypatch.setattr(airflow_run, "REQUEST_TIMEOUT", 0.3)
        assert airflow_run.run(airflow_run.Api(patience=30, retry_wait=0), "d", "r") == 0
    finally:
        server.shutdown()
    out = capsys.readouterr().out
    assert re.search(r"Airflow's API did not answer in \d+ s \(timed out\): trying again until it has been "
                     r"silent for 30 s\n", out)
    assert re.search(r"Airflow's API answers again, after \d+ s\n", out)
    assert "d: run r succeeded" in out
    assert len(polls) == 3                    # running; the one that timed out; success
    assert PASSWORD not in out


def test_a_trigger_whose_answer_was_lost_counts_as_triggered_when_its_retry_finds_the_run(monkeypatch, capsys):
    def answer(method, path, nth):
        if method == "POST" and path == "/api/v2/dags/d/dagRuns":
            if nth == 1:
                raise TimeoutError("timed out")      # the run was created; its answer was lost
            raise http_error(409)                    # so the second try finds it there
        return airflow(method, path, nth)

    send = Scripted(answer)
    monkeypatch.setattr(airflow_run.Api, "send", send)
    assert airflow_run.run(airflow_run.Api(retry_wait=0), "d", "r") == 0
    assert send.count("POST", "/api/v2/dags/d/dagRuns") == 2
    assert "d: run r succeeded" in capsys.readouterr().out


def test_a_conflict_on_the_first_try_is_an_error(monkeypatch):
    def answer(method, path, nth):
        if method == "POST" and path == "/api/v2/dags/d/dagRuns":
            raise http_error(409)                    # a run with this id existed before
        return airflow(method, path, nth)

    monkeypatch.setattr(airflow_run.Api, "send", Scripted(answer))
    with pytest.raises(urllib.error.HTTPError) as raised:
        airflow_run.run(airflow_run.Api(retry_wait=0), "d", "r")
    assert raised.value.code == 409


@pytest.mark.parametrize("silent_at, run_is", [
    (("PATCH", "/api/v2/dags/d"), "was not triggered"),
    (("POST", "/api/v2/dags/d/dagRuns"), "may or may not have been triggered"),
    (("GET", RUN), "goes on in the scheduler without this script"),
])
def test_an_api_silent_past_the_limit_ends_with_a_sentence_and_the_run_url_not_a_traceback(
        monkeypatch, capsys, silent_at, run_is):
    def answer(method, path, nth):
        if (method, path) == silent_at:
            raise urllib.error.URLError(ConnectionRefusedError(111, "Connection refused"))
        return airflow(method, path, nth)

    send = Scripted(answer)
    monkeypatch.setattr(airflow_run.Api, "send", send)
    assert airflow_run.run(airflow_run.Api(patience=0.2, retry_wait=0.05), "d", "r") == airflow_run.UNREACHABLE
    assert send.count(*silent_at) > 1                # tried again before giving up
    err = capsys.readouterr().err
    assert re.search(r"d: Airflow's API has not answered for \d+ s \(\[Errno 111\] Connection refused\)\. "
                     r"Gave up: run r " + run_is, err)
    assert err.rstrip().endswith("See http://127.0.0.1:8080/dags/d/runs/r")
    assert "Traceback" not in err and PASSWORD not in err


@pytest.mark.parametrize("code", [404, 500])
def test_an_answer_that_says_no_is_not_tried_again(monkeypatch, code):
    def answer(method, path, nth):
        if path == RUN:
            raise http_error(code)
        return airflow(method, path, nth)

    send = Scripted(answer)
    monkeypatch.setattr(airflow_run.Api, "send", send)
    with pytest.raises(urllib.error.HTTPError):
        airflow_run.run(airflow_run.Api(retry_wait=0), "d", "r")
    assert send.count("GET", RUN) == 1


@pytest.mark.parametrize("code", [502, 503, 504])
def test_a_busy_gateway_is_tried_again(monkeypatch, code):
    def answer(method, path, nth):
        if path == RUN and nth == 1:
            raise http_error(code)
        return airflow(method, path, nth - (path == RUN))

    monkeypatch.setattr(airflow_run.Api, "send", Scripted(answer))
    assert airflow_run.run(airflow_run.Api(retry_wait=0), "d", "r") == 0


def test_ready_keeps_its_own_loop_for_a_server_that_is_still_starting(monkeypatch, capsys):
    def answer(method, path, nth):
        if path == "/auth/token":
            return {"access_token": "token"}
        raise TimeoutError("timed out")

    send = Scripted(answer)
    monkeypatch.setattr(airflow_run.Api, "send", send)
    # No patient retry inside: one look at the DAG, then the import errors, then ready's own limit
    assert airflow_run.ready(airflow_run.Api(retry_wait=60), ["d"], timeout=-1) == 1
    assert send.calls == [("POST", "/auth/token"), ("GET", "/api/v2/dags/d"), ("GET", "/api/v2/importErrors")]
    assert "did not answer" not in capsys.readouterr().out
