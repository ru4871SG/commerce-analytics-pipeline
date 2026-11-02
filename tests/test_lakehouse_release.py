"""scripts/lakehouse_release.py: what `deploy` asks of the verification. The full release, deploy
and verify are exercised by `scripts/local.sh lakehouse release|deploy|verify -t local` with the
real CLI."""
from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("lakehouse_release", REPO / "scripts" / "lakehouse_release.py")
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


def test_a_token_the_cli_echoes_never_reaches_the_log(capsys):
    release.say("STOPPED: databricks bundle validate failed (1):\n" + "a" * 2588 + "." + "b" * 342 + ". Env: X, Y")
    printed = capsys.readouterr().out
    assert "aaaa" not in printed and "bbbb" not in printed
    assert printed == "STOPPED: databricks bundle validate failed (1):\n<token removed>. Env: X, Y\n"


def test_ordinary_output_is_left_as_it_is(capsys):
    line = "wheel " + "0" * 64 + " in /a/long/path/" + "/".join(["folder"] * 30) + " for job web_events_spark"
    release.say(line)
    assert capsys.readouterr().out == line + "\n"


def test_the_production_jobs_wait_for_a_workspace():
    jobs = yaml.safe_load((REPO / ".github" / "workflows" / "lakehouse.yml").read_text())["jobs"]
    assert jobs["release"]["if"] == "github.event_name == 'push' && vars.DATABRICKS_HOST != ''"
    assert jobs["deploy"]["needs"] == "release"     # so it is skipped with it


@pytest.mark.parametrize("target, creates_tables", [("local", True), ("dev", False), ("prod", False)])
def test_only_a_local_deploy_creates_the_tables_its_health_check_reads(tmp_path, monkeypatch, target, creates_tables):
    verified = []
    monkeypatch.setattr(release, "_load", lambda release_dir, target: {"release": "r"})
    monkeypatch.setattr(release, "still_holds", lambda evidence, release_dir, target: None)
    monkeypatch.setattr(release, "cli", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0, "", ""))
    monkeypatch.setattr(release, "verify", lambda target, release_dir, setup: verified.append(setup) or 0)

    assert release.deploy(target, tmp_path) == 0
    assert verified == [creates_tables]
