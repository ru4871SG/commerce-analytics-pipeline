"""The fixed CI scenario's settings in scripts/local.sh, and .github/workflows/pipeline.yml."""
from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from crm_pipeline import simulator

REPO = Path(__file__).resolve().parents[1]
LOCAL_SH = REPO / "scripts" / "local.sh"
SCRIPT = LOCAL_SH.read_text()
WORKFLOWS = REPO / ".github" / "workflows"
PIPELINE = yaml.safe_load((WORKFLOWS / "pipeline.yml").read_text())
PIPELINE_TEXT = (WORKFLOWS / "pipeline.yml").read_text()


def setting(name: str) -> str:
    """A constant of scripts/local.sh, as written: NAME="value"."""
    return re.search(rf'^{name}="([^"]+)"', SCRIPT, re.MULTILINE).group(1)


def local_sh(*args: str, **environment: str) -> subprocess.CompletedProcess:
    return subprocess.run([str(LOCAL_SH), *args], env={**os.environ, **environment}, capture_output=True, text=True,
                          timeout=60)


def steps(job: str) -> list[dict]:
    return PIPELINE["jobs"][job]["steps"]


def test_the_scenario_starts_where_the_seed_ends_and_adds_three_days():
    seed_end, scenario_end = (date.fromisoformat(setting(name)) for name in ("SEED_END", "CI_SCENARIO_END"))
    assert seed_end + timedelta(days=1) == simulator.FIRST_DAY      # the simulator's first day follows the seed
    assert scenario_end - seed_end == timedelta(days=3)


def test_a_simulate_until_that_is_not_a_day_stops_any_command_before_it_starts():
    result = local_sh("status", CRM_SIMULATE_UNTIL="tomorrow")
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "CRM_SIMULATE_UNTIL must be a day as YYYY-MM-DD, not 'tomorrow'.\n"


def test_the_scenario_refuses_an_option_it_does_not_know_before_it_resets_anything():
    result = local_sh("ci-scenario", "--rest")
    assert result.returncode == 2
    assert result.stderr.endswith("ci-scenario [--reset]\n")
    # The option is read before the first command that could change the machine
    body = SCRIPT[SCRIPT.index("ci_scenario() {"):]
    assert body.index('exit 2') < min(body.index(command) for command in ("source_db_reset", "warehouse_reset", "rm -rf"))


def test_a_command_it_does_not_know_is_an_error_not_the_help():
    """A mistyped command used to print the help and exit 0: in a workflow, a green step that ran nothing."""
    result = local_sh("ci-scenaro", "--reset")
    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr.startswith("Unknown command: ci-scenaro\nOne local entry point for the whole demo.")
    for asked in (["help"], ["-h"], ["--help"], []):
        result = local_sh(*asked)
        assert (result.returncode, result.stderr) == (0, ""), asked
        assert result.stdout.startswith("One local entry point for the whole demo."), asked


def test_the_days_before_the_last_are_written_in_one_process_and_the_last_by_a_run_of_the_dag():
    """A run of crm_daily_activity takes seconds to start and the days since the seed grow by one a
    day, so only the last missing day is a run of the DAG; the scenario's log must still show a
    line per brand and day."""
    body = SCRIPT[SCRIPT.index("simulate_days() {"):SCRIPT.index("LOCAL_WORKSPACE_PORT=")]
    airflow = body[body.index("# A run of crm_daily_activity"):]
    assert "while " not in airflow and "for " not in airflow.replace("for $", "")       # no run per day
    assert airflow.count("airflow dags test crm_daily_activity") == 1
    assert (airflow.index('lakehouse_python -m crm_pipeline.simulator --from "$first" --to "$before"')
            < airflow.index('airflow dags test crm_daily_activity "$logical"'))
    assert 'logical="$(TZ=America/New_York date -d "$through + 1 day 07:00" --iso-8601=seconds)"' in airflow
    scenario = SCRIPT[SCRIPT.index("ci_scenario() {"):SCRIPT.index("ci_summary() {")]
    assert "grep -c ' new customers ' \"$CI_DIR/simulate.log\"" in scenario
    assert '"%s %s: %s new customers (' in (REPO / "dags" / "crm_pipeline" / "simulator.py").read_text()

    shown = re.search(r'grep -oE "(\(\$keys\) \[0-9\][^"]+)" "\$REPO/\.local/simulate\.log" \\\n +\| sed \'([^\']+)\'\n    else',
                      airflow)
    pattern = re.compile(shown.group(1).replace("$keys", "wecare|greenz|broomx"))
    assert shown.group(2) == r"s/ \[crm_pipeline.*//; s/^/  /"
    day = "wecare 2026-10-01: 3 new customers (2 first orders), 0 renewals"
    for line in (f"2026-11-11T20:09:14.342248Z [info     ] {day} [crm_pipeline.simulator] loc=simulator.py:409", day):
        assert re.sub(r" \[crm_pipeline.*", "", pattern.search(line).group(0)) == day
    assert pattern.search("[info     ] wecare 2026-10-01 is already in the CRM: nothing to do. [crm_pipeline.simulator]")
    assert not pattern.search("[info     ] wecare 2026-10-01 clickstream: {'lines': 5} [crm_pipeline.clickstream]")


def test_the_pipeline_commands_start_the_local_services_but_only_on_the_emulators():
    """With PostgreSQL or fakesnow stopped no task fails for good: each waits for its retries and
    the run stands still. Against a real warehouse the same commands must start nothing here."""
    services = SCRIPT[SCRIPT.index("pipeline_services() {"):SCRIPT.index("run_report() {")]
    assert "if emulator_target; then\n        local_services_up\n    fi" in services
    airflow = SCRIPT[SCRIPT.index("\n    pipeline)\n"):SCRIPT.index("\n    dagster)\n")]
    assert airflow.index("claim_orchestrator airflow") < airflow.index("pipeline_services") < airflow.index("run_report airflow")
    dagster = SCRIPT[SCRIPT.index("\n            pipeline)\n"):]
    assert dagster.index("pipeline_services") < dagster.index("run_report dagster")
    for function in ("run_demo() {", "full_run() {"):
        assert "local_services_up" in SCRIPT[SCRIPT.index(function):SCRIPT.index("\n}\n", SCRIPT.index(function))]


def test_a_container_engine_other_than_podman_or_docker_is_refused():
    result = local_sh("source-db", "up", CRM_CONTAINER_ENGINE="nerdctl")
    assert result.returncode == 1
    assert "CRM_CONTAINER_ENGINE must be podman or docker, not 'nerdctl'." in result.stderr


def test_the_scenario_needs_nothing_outside_this_machine():
    body = SCRIPT[SCRIPT.index("ci_scenario() {"):SCRIPT.index("ci_summary() {")]
    assert body.index("unset AIRFLOW_CONN_ECB_FX_API") < body.index("ci_step ")
    evidence = SCRIPT[SCRIPT.index("ci_report_evidence() {"):SCRIPT.index("ci_activation_evidence() {")]
    assert '[[ "$fx" == "FX rates: skipped" ]] || problems=' in evidence


def test_the_scenario_reads_the_three_simulated_days_from_the_write_ahead_log():
    body = SCRIPT[SCRIPT.index("ci_scenario() {"):SCRIPT.index("ci_summary() {")]
    assert "unset AIRFLOW_CONN_CRM_SOURCE_CDC" not in body
    assert 'ci_report_evidence airflow-report-first "Airflow, first load" spark nothing\n' in body
    assert 'ci_report_evidence airflow-report-incremental "Airflow, incremental" pandas changes\n' in body
    assert 'ci_report_evidence dagster-report "Dagster, report" "" nothing\n' in body
    evidence = SCRIPT[SCRIPT.index("ci_report_evidence() {"):SCRIPT.index("ci_activation_evidence() {")]
    assert "CDC orders: [a-z0-9_]+ (captured [0-9]+ changes|has nothing new)" in evidence
    assert 'problems="$problems the CDC did not capture changes of each of the $brands brands;"' in evidence
    assert "the CDC did not read each of the $brands brands' slots and find nothing new;" in evidence
    capture = (REPO / "dags" / "crm_pipeline" / "cdc.py").read_text()
    assert '"CDC orders: %s captured %s changes (' in capture and '"CDC orders: %s has nothing new.' in capture
    line = next(line for line in (REPO / ".env.local.example").read_text().splitlines()
                if line.startswith("AIRFLOW_CONN_CRM_SOURCE_CDC="))
    assert "@127.0.0.1:5433" in line 


def test_a_dagster_job_starts_where_no_env_file_can_put_a_setting_back():
    job = SCRIPT[SCRIPT.index("run_dagster_job() {"):SCRIPT.index("run_report() {")]
    assert job.index('cd "$REPO/dagster"') < job.index("dagster\" job execute")
    assert not (REPO / "dagster" / ".env").exists()


def ensure_local_env(repo: Path) -> subprocess.CompletedProcess:
    """The function install calls, run on a folder of its own."""
    function = SCRIPT[SCRIPT.index("ensure_local_env() {"):]
    function = function[:function.index("\n}\n") + 3]
    return subprocess.run(["bash", "-c", f'set -euo pipefail\nREPO="$1"\n{function}\nensure_local_env', "bash", str(repo)],
                          capture_output=True, text=True, timeout=30)


def test_install_adds_the_example_s_new_settings_to_an_env_that_exists(tmp_path):
    (tmp_path / ".env.local.example").write_text(
        "# a comment with KEPT=in it\n"
        "KEPT='from the example'\n"
        "SWITCHED_OFF=on\n"
        "AIRFLOW_CONN_NEW_API='{\"conn_type\": \"http\", \"extra\": {\"base_url\": \"https://example.org\"}}'\n")
    (tmp_path / ".env").write_text(
        "KEPT='mine'\n"
        "# SWITCHED_OFF=on\n"
        "AIRFLOW_PASSWORD=a\nSUPERSET_PASSWORD=b")          # no newline at the end
    result = ensure_local_env(tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout == ("Added to .env from .env.local.example: AIRFLOW_CONN_NEW_API. "
                             "What .env already had is unchanged.\n")
    assert (tmp_path / ".env").read_text() == (
        "KEPT='mine'\n"                    # never overwritten
        "# SWITCHED_OFF=on\n" 
        "AIRFLOW_PASSWORD=a\nSUPERSET_PASSWORD=b\n"
        "AIRFLOW_CONN_NEW_API='{\"conn_type\": \"http\", \"extra\": {\"base_url\": \"https://example.org\"}}'\n")
    assert (tmp_path / ".env").stat().st_mode & 0o777 == 0o600
    again = ensure_local_env(tmp_path)
    assert (again.returncode, again.stdout) == (0, "")


def test_a_new_machine_gets_the_whole_example(tmp_path):
    example = "A=1\nAIRFLOW_PASSWORD=a\nSUPERSET_PASSWORD=b\n"
    (tmp_path / ".env.local.example").write_text(example)
    result = ensure_local_env(tmp_path)
    assert (result.returncode, result.stdout) == (0, "")
    assert (tmp_path / ".env").read_text() == example


def test_the_ecb_connection_of_the_local_example_holds_no_secret():
    line = next(line for line in (REPO / ".env.local.example").read_text().splitlines()
                if line.startswith("AIRFLOW_CONN_ECB_FX_API="))
    connection = json.loads(line.split("=", 1)[1].strip("'"))
    assert connection == {"conn_type": "http", "extra": {"base_url": "https://data-api.ecb.europa.eu",
                                                         "endpoint": "/service/data/EXR"}}


@pytest.mark.parametrize("tool", ["SHELLCHECK", "ACTIONLINT", "RUFF", "SQLFLUFF"])
def test_every_lint_tool_is_pinned_to_one_version(tool):
    assert re.fullmatch(r"\d+(\.\d+){2,3}", setting(f"{tool}_VERSION"))


def test_every_action_is_pinned_by_commit_with_the_pins_the_lakehouse_workflow_uses():
    uses = re.compile(r"uses: (\S+)@(\S+) # (v\S+)")
    lakehouse = {action: (sha, version) for action, sha, version in
                 uses.findall((WORKFLOWS / "lakehouse.yml").read_text())}
    pinned = uses.findall(PIPELINE_TEXT)
    assert len(pinned) == PIPELINE_TEXT.count("uses:")
    for action, sha, version in pinned:
        assert re.fullmatch(r"[0-9a-f]{40}", sha), f"{action} is not pinned by a commit SHA"
        if action in lakehouse:    
            assert lakehouse[action] == (sha, version), f"{action} is pinned differently from lakehouse.yml"
    assert {action for action, _, _ in pinned} & set(lakehouse)  


def test_the_workflow_reads_only_and_holds_no_secret():
    assert PIPELINE["permissions"] == {"contents": "read"}
    assert "permissions" not in PIPELINE["jobs"]["lint"]
    assert PIPELINE["jobs"]["pipeline"]["permissions"] == {"contents": "read", "actions": "read"}
    assert "secrets." not in PIPELINE_TEXT
    writers = {name: job["permissions"] for name, job in PIPELINE["jobs"].items()
               if "write" in job.get("permissions", {}).values()}
    assert writers == {"pages": {"pages": "write", "id-token": "write"}}
    assert PIPELINE_TEXT.count("id-token") == 1 and PIPELINE_TEXT.count(": write") == 2


SITE_ONLY = "github.event_name == 'push' && vars.PUBLISH_SITE == 'true'"


def test_the_site_is_published_from_main_only_when_the_repository_asks_for_it():
    by_name = {step["name"]: step for step in steps("pipeline") if "name" in step}
    build = by_name["The site of this run (scripts/local.sh site)"]
    upload = by_name["Hand the site to the pages job"]
    pages = PIPELINE["jobs"]["pages"]
    assert build["if"] == upload["if"] == pages["if"] == SITE_ONLY
    assert build["run"] == "scripts/local.sh site"
    assert build["env"] == {"CRM_SITE_BROWSER_DEPS": "true"} 
    assert upload["uses"].startswith("actions/upload-pages-artifact@") and upload["with"] == {"path": ".local/site"}
    assert 'SITE_DIR="$REPO/.local/site"' in SCRIPT
    names = [step.get("name") for step in steps("pipeline")]
    assert names.index("Publish main's gold fingerprint, for pull requests to compare with") < names.index(build["name"])
    assert names[-2:] == [build["name"], upload["name"]]


def test_the_pages_job_only_deploys_what_the_pipeline_job_built():
    pages = PIPELINE["jobs"]["pages"]
    assert pages["needs"] == "pipeline"
    (deploy,) = pages["steps"]
    assert deploy["uses"].startswith("actions/deploy-pages@") and deploy["id"] == "deployment"
    assert "run" not in deploy
    assert pages["environment"] == {"name": "github-pages", "url": "${{ steps.deployment.outputs.page_url }}"}
    assert "checkout" not in str(pages)


def test_it_runs_for_every_pull_request_and_every_push_to_main():
    triggers = PIPELINE[True]       # YAML reads the key `on` as a boolean
    assert triggers == {"pull_request": None, "push": {"branches": ["main"]}}


def test_the_pipeline_job_installs_tests_then_runs_the_scenario_and_the_dashboard_check():
    commands = [step["run"] for step in steps("pipeline") if "run" in step]
    assert commands[:4] == [
        "scripts/local.sh install",
        "scripts/local.sh test",
        "scripts/local.sh ci-scenario --reset",
        "scripts/local.sh dashboard check",
    ]
    assert all("if" not in step for step in steps("pipeline")[:7])


def test_the_lint_job_compares_with_the_base_commit():
    checkout, _, lint = steps("lint")
    assert checkout["with"] == {"fetch-depth": 0}   
    assert lint["env"] == {"BASE": "${{ github.event.pull_request.base.sha || github.event.before }}"}
    assert lint["run"] == 'scripts/local.sh lint "$BASE"'


def test_main_publishes_the_fingerprint_a_pull_request_compares_with():
    by_name = {step["name"]: step for step in steps("pipeline") if "name" in step}
    publish = by_name["Publish main's gold fingerprint, for pull requests to compare with"]
    assert publish["if"] == "github.event_name == 'push'"
    assert publish["with"]["name"] == "gold-fingerprint"
    assert publish["with"]["path"] == ".local/ci-scenario/gold-airflow.json"
    assert '"$CI_DIR/gold-airflow.json"' in SCRIPT   

    download = by_name["Main's gold fingerprint, from its latest successful run"]
    assert download["if"] == "always() && github.event_name == 'pull_request'"
    for flag in ("--workflow pipeline.yml", "--branch main", "--event push", "--status success",
                 "--name gold-fingerprint"):
        assert flag in download["run"]

    summary = by_name["Summary, with the gold diff against main"]
    assert summary["if"] == "always()" 
    assert 'ci-summary "$RUNNER_TEMP/base/gold-airflow.json"' in summary["run"]
