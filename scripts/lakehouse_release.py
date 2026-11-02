#!/usr/bin/env python3
"""Release, deploy and verify the lakehouse bundle, with the evidence that ties them together.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BUNDLE = REPO / "lakehouse"
DIST = BUNDLE / "dist"
RELEASES = REPO / ".local" / "releases"
CLI = os.environ.get("DATABRICKS_CLI", str(REPO / ".local" / "bin" / "databricks"))
SPARK_PYTHON = REPO / ".venv-spark" / "bin" / "python"
LOCAL_WORKSPACE = "http://127.0.0.1:8770"
JOB = "web_events_spark"
# What a release is made of: a change to any of these, uncommitted, makes the tree dirty
RELEASED_PATHS = ("lakehouse", "requirements.txt", "scripts/lakehouse_release.py", ".github/workflows/lakehouse.yml")
PIP_AUDIT = "pip-audit==2.10.1"
UNCHANGED = {"skip", "noop", ""}
SECRET_PATTERNS = {
    "AWS access key": r"AKIA[0-9A-Z]{16}",
    "Databricks token": r"dapi[0-9a-f]{32}",
    "private key": r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
    "client secret": r"(?i)client_secret\s*[:=]\s*['\"]?[A-Za-z0-9_\-]{16,}",
}
# What no log may show: GitHub's OIDC request token, which the Databricks CLI echoes when it cannot
# authenticate, whole or cut short. No useful message holds 100 base64url characters in a row.
TOKEN_LIKE = re.compile(r"[A-Za-z0-9_-]{100,}(?:\.[A-Za-z0-9_-]+)*")

sys.path.insert(0, str(BUNDLE / "src"))
from crm_lakehouse import contract_fingerprint  # noqa: E402


class ReleaseError(Exception):
    """A check failed: the release, the deployment or the verification stops here."""


def say(text: str) -> None:
    print(TOKEN_LIKE.sub("<token removed>", text), flush=True)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(command: list[str], env: dict | None = None, cwd: Path = REPO, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True)
    if check and result.returncode:
        raise ReleaseError(f"{' '.join(command)} failed ({result.returncode}):\n{result.stderr[-3000:] or result.stdout[-3000:]}")
    return result


def git(*args: str) -> str:
    return run(["git", *args]).stdout.strip()


# --- the CLI, signed in for the target -----------------------------------------------------

def cli_env(target: str, release_dir: Path) -> dict:
    env = dict(os.environ)
    if target == "local":
        # The local workspace takes any token; keep the caller's profiles and hosts out of it
        env["DATABRICKS_HOST"] = LOCAL_WORKSPACE
        env["DATABRICKS_TOKEN"] = "local-workspace"
        env["DATABRICKS_CONFIG_FILE"] = str(release_dir / "no-databrickscfg")
        env.setdefault("BUNDLE_VAR_location", str(REPO / ".local" / "lakehouse"))
        try:
            urllib.request.urlopen(f"{LOCAL_WORKSPACE}/api/2.0/preview/scim/v2/Me", timeout=3).read()
        except OSError as exc:
            raise ReleaseError(f"The local workspace does not answer ({exc}): scripts/local.sh lakehouse workspace up") from exc
    elif env.get("DATABRICKS_TOKEN"):
        raise ReleaseError("DATABRICKS_TOKEN is set: a personal access token is long-lived. Use OAuth instead: "
                           "GitHub OIDC in CI, `databricks auth login` by hand.")
    return env


def cli(target: str, release_dir: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return run([CLI, "bundle", *args, "-t", target], env=cli_env(target, release_dir), cwd=BUNDLE, check=check)


def cli_json(target: str, release_dir: Path, *args: str) -> dict:
    return json.loads(cli(target, release_dir, *args, "-o", "json").stdout)


# --- checks --------------------------------------------------------------------------------

def schema_errors() -> list[str]:
    """The bundle's files against the CLI's JSON schema, offline. The schema's Go regular
    expressions use \\p{L} and \\p{N}, which Python's re spells differently."""
    import jsonschema
    import yaml

    text = run([CLI, "bundle", "schema"]).stdout
    text = text.replace(r"[\\p{L}\\p{N}]", r"[^\\W_]").replace(r"\\p{L}", r"[^\\W\\d_]")
    validator = jsonschema.Draft7Validator(json.loads(text))
    errors = []
    for path in [BUNDLE / "databricks.yml", *sorted((BUNDLE / "resources").glob("*.yml"))]:
        for error in validator.iter_errors(yaml.safe_load(path.read_text())):
            errors.append(f"{path.relative_to(REPO)}: {'.'.join(map(str, error.path))}: {error.message[:200]}")
    return errors


def wheel_fingerprint(wheel: Path) -> str:
    """contract_fingerprint() of the code inside the wheel, computed the same way."""
    digest = hashlib.sha256()
    with zipfile.ZipFile(wheel) as archive:
        names = sorted(n for n in archive.namelist() if re.fullmatch(r"crm_lakehouse/[^/]+\.py", n))
        for name in names:
            digest.update(name.split("/", 1)[1].encode())
            digest.update(archive.read(name))
    return digest.hexdigest()[:16]


def build_wheel(out_dir: Path, epoch: str) -> Path:
    shutil.rmtree(out_dir, ignore_errors=True)
    run(["uv", "build", "--wheel", "--out-dir", str(out_dir), str(BUNDLE)],
        env={**os.environ, "SOURCE_DATE_EPOCH": epoch})
    wheels = list(out_dir.glob("crm_lakehouse-*.whl"))
    if len(wheels) != 1:
        raise ReleaseError(f"Expected one wheel in {out_dir}, found {wheels}")
    return wheels[0]


def resources(plan: dict) -> dict:
    return {key: value.get("action", "") for key, value in plan.get("plan", {}).items()}


# What a job's remote state carries besides its settings
JOB_METADATA = {"job_id", "created_time", "creator_user_name", "run_as_user_name"}


def after_plan(entry: dict) -> dict:
    """The state a resource will have once the plan is applied: the new state when the plan changes
    it, the remote one when it leaves it alone (a "skip" carries no new state)."""
    return ((entry.get("new_state") or {}).get("value")) or entry.get("remote_state") or {}


def job_settings(plan: dict) -> dict:
    state = after_plan(plan["plan"][f"resources.jobs.{JOB}"])
    return {key: value for key, value in state.items() if key not in JOB_METADATA}


def policy_checks(plan: dict, target: str, wheel_name: str) -> list[str]:
    """What production must look like, checked on the plan before anything is deployed. Returns
    the violations."""
    settings, problems = job_settings(plan), []
    if settings.get("job_clusters") or any("existing_cluster_id" in t or "new_cluster" in t for t in settings.get("tasks", [])):
        problems.append("the job must run on serverless compute: no all-purpose or job clusters")
    dependencies = [d for e in settings.get("environments", []) for d in e.get("spec", {}).get("dependencies", [])]
    if not any(d.endswith("/" + wheel_name) for d in dependencies):
        problems.append(f"the job does not install the released wheel {wheel_name}: {dependencies}")
    if settings.get("max_concurrent_runs") != 1:
        problems.append("batches run one at a time: max_concurrent_runs must be 1")
    if target == "prod" and not (settings.get("run_as") or {}).get("service_principal_name"):
        problems.append("production runs as a service principal (run_as)")
    grants = after_plan(plan["plan"].get(f"resources.jobs.{JOB}.permissions", {}))
    for grant in grants.get("__embed__", grants if isinstance(grants, list) else []):
        level = grant.get("level") or grant.get("permission_level")
        if grant.get("group_name") in ("users", "account users"):
            problems.append(f"everyone has {level} on the job")
        if level not in ("CAN_MANAGE_RUN", "IS_OWNER", "CAN_VIEW"):
            problems.append(f"{level} granted on the job to {grant}")
    return problems


def wheel_path(settings: dict) -> str | None:
    return next((d for e in settings.get("environments", []) for d in e.get("spec", {}).get("dependencies", [])
                 if d.endswith(".whl")), None)


def version_check(target: str, release_dir: Path, plan: dict, fingerprint: str) -> list[str]:
    """A wheel with new code must have a new version. The job's settings name the wheel by its path,
    which a new build of the same version does not change, so the plan finds nothing to update and
    serverless compute may go on using the environment it built from the old file."""
    path = wheel_path(job_settings(plan))
    if not path:
        return []
    deployed = release_dir / "previously_deployed.whl"
    exported = run([CLI, "workspace", "export", path, "--format", "AUTO", "--file", str(deployed)],
                   env=cli_env(target, release_dir), check=False)
    if exported.returncode:
        return []                              # nothing deployed there yet
    previous = wheel_fingerprint(deployed)
    deployed.unlink()
    if previous != fingerprint:
        return [f"{path.rsplit('/', 1)[-1]} is deployed already with other code ({previous}): raise the version in "
                f"lakehouse/pyproject.toml, so the job's environment changes with it"]
    return []


def secret_findings(*texts: tuple[str, str]) -> list[str]:
    return [f"{where}: looks like a {name}" for where, text in texts
            for name, pattern in SECRET_PATTERNS.items() if re.search(pattern, text)]


def bundle_texts() -> list[tuple[str, str]]:
    paths = [BUNDLE / "databricks.yml", *sorted((BUNDLE / "resources").glob("*.yml")),
             *sorted((BUNDLE / "src" / "crm_lakehouse").glob("*.py"))]
    return [(str(p.relative_to(REPO)), p.read_text()) for p in paths]


# --- commands ------------------------------------------------------------------------------

def validate(target: str) -> int:
    errors = schema_errors()
    for error in errors:
        say(f"schema: {error}")
    if errors:
        return 1
    say("schema: OK, every bundle file matches the CLI's JSON schema")
    scratch = Path(tempfile.mkdtemp())
    # The bundle's artifact is dist/crm_lakehouse-*.whl, which only a release builds. Before the
    # first one, build a wheel as a release would, for this check only, and remove it after.
    borrowed = None
    if not list(DIST.glob("crm_lakehouse-*.whl")):
        made_dist = not DIST.exists()
        wheel = build_wheel(scratch / "dist", git("log", "-1", "--format=%ct"))
        DIST.mkdir(exist_ok=True)
        borrowed = Path(shutil.copy2(wheel, DIST / wheel.name))
        say(f"wheel: no release has built one yet; built {wheel.name} for this check, removed after it")
    try:
        result = cli(target, scratch, "validate", check=False)
    finally:
        if borrowed:
            borrowed.unlink(missing_ok=True)
            if made_dist:
                shutil.rmtree(DIST, ignore_errors=True)
        shutil.rmtree(scratch, ignore_errors=True)
    say(result.stdout + result.stderr)
    return result.returncode


def release(target: str, allow_dirty: bool, skip_tests: bool, offline: bool = False) -> int:
    started = datetime.now(timezone.utc)
    release_dir = RELEASES / f"{target}-{started:%Y%m%dT%H%M%SZ}"
    release_dir.mkdir(parents=True)
    commit, dirty = git("rev-parse", "HEAD"), bool(git("status", "--porcelain", "--", *RELEASED_PATHS))
    if dirty and (target == "prod" or not allow_dirty):
        raise ReleaseError(f"Uncommitted changes in {', '.join(RELEASED_PATHS)}: commit them, or for a local release "
                           "pass --allow-dirty (recorded in the evidence)")
    epoch = git("log", "-1", "--format=%ct")
    say(f"Release {release_dir.name}: commit {commit[:12]}{' (with uncommitted changes)' if dirty else ''}")

    errors = schema_errors()
    if errors:
        raise ReleaseError("The bundle does not match the CLI's schema:\n" + "\n".join(errors))
    say("  schema      OK")

    wheel = build_wheel(DIST, epoch)
    again = build_wheel(release_dir / "rebuild", epoch)
    reproducible = sha256(wheel) == sha256(again)
    shutil.rmtree(again.parent)
    if not reproducible:
        raise ReleaseError("Two builds of the same source gave different wheels: the build is not reproducible.")
    shutil.copy2(wheel, release_dir / wheel.name)
    source_fingerprint, built_fingerprint = contract_fingerprint(), wheel_fingerprint(wheel)
    if source_fingerprint != built_fingerprint:
        raise ReleaseError(f"The wheel's code ({built_fingerprint}) is not the source's ({source_fingerprint}).")
    say(f"  wheel       {wheel.name} sha256 {sha256(wheel)[:16]}..., rebuilt identical, code {built_fingerprint}")

    tests = {"skipped": True}
    if not skip_tests:
        result = run([str(SPARK_PYTHON), "-m", "pytest", "lakehouse/tests", "-q", "-p", "no:cacheprovider"], check=False)
        (release_dir / "tests.log").write_text(result.stdout + result.stderr)
        summary = (result.stdout.strip().splitlines() or [""])[-1]
        tests = {"passed": result.returncode == 0, "summary": summary}
        if result.returncode:
            raise ReleaseError(f"The lakehouse tests failed: {summary} (see {release_dir / 'tests.log'})")
        say(f"  tests       {summary}")
    elif target == "prod":
        raise ReleaseError("A production release runs the tests.")

    requirements = release_dir / "requirements-spark.txt"
    section = re.search(r"# \[spark\][^\n]*\n(.*?)(?=\n# \[|\Z)", (REPO / "requirements.txt").read_text(), re.S).group(1)
    requirements.write_text("\n".join(line for line in section.splitlines() if line and not line.startswith("#")) + "\n")
    audit = run(["uv", "tool", "run", PIP_AUDIT, "-r", str(requirements), "--progress-spinner", "off"], check=False)
    (release_dir / "audit.log").write_text(audit.stdout + audit.stderr)
    audit_result = {"tool": PIP_AUDIT, "clean": audit.returncode == 0,
                    "summary": (audit.stdout.strip() or audit.stderr.strip()).splitlines()[-1:] or [""]}
    if audit.returncode and target == "prod":
        raise ReleaseError(f"The dependency audit found problems (or could not run): see {release_dir / 'audit.log'}")
    say(f"  audit       {'no known vulnerabilities' if audit.returncode == 0 else 'FINDINGS, see audit.log'}")

    if offline:
        # A pull request: no workspace credentials, so no plan. The checks that need none are done.
        secrets = secret_findings(*bundle_texts())
        if secrets:
            raise ReleaseError("Security checks failed:\n  " + "\n  ".join(secrets))
        evidence = {"release": release_dir.name, "target": target, "created_at": started.isoformat(), "offline": True,
                    "git": {"commit": commit, "dirty": dirty},
                    "wheel": {"name": wheel.name, "sha256": sha256(wheel), "reproducible": reproducible,
                              "code_fingerprint": built_fingerprint},
                    "source_code_fingerprint": source_fingerprint, "tests": tests,
                    "security": {"dependency_audit": audit_result, "secret_scan": "clean",
                                 "policy_checks": "not run: no plan offline"}}
        (release_dir / "evidence.json").write_text(json.dumps(evidence, indent=1))
        say(f"Offline release checks passed (no plan: no workspace). Evidence in {release_dir}")
        return 0
    config = cli_json(target, release_dir, "validate")
    plan = cli_json(target, release_dir, "plan")
    (release_dir / "config.json").write_text(json.dumps(config, indent=1, sort_keys=True))
    (release_dir / "plan.json").write_text(json.dumps(plan, indent=1))
    secrets = secret_findings(*bundle_texts(), ("plan.json", json.dumps(plan)), ("config.json", json.dumps(config)))
    policies = policy_checks(plan, target, wheel.name) + version_check(target, release_dir, plan, built_fingerprint)
    if secrets or policies:
        raise ReleaseError("Security checks failed:\n  " + "\n  ".join(secrets + policies))
    say(f"  plan        {resources(plan)}")
    say("  security    no secrets in the bundle, the configuration or the plan; policy checks passed")

    evidence = {
        "release": release_dir.name, "target": target, "created_at": started.isoformat(),
        "git": {"commit": commit, "dirty": dirty},
        "wheel": {"name": wheel.name, "sha256": sha256(wheel), "reproducible": reproducible,
                  "code_fingerprint": built_fingerprint},
        "source_code_fingerprint": source_fingerprint,
        "tests": tests,
        "security": {"dependency_audit": audit_result, "secret_scan": "clean", "policy_checks": "passed"},
        "bundle": {"cli_version": plan.get("cli_version"), "config_sha256": sha256(release_dir / "config.json"),
                   "plan_sha256": sha256(release_dir / "plan.json"), "resources": resources(plan)},
    }
    (release_dir / "evidence.json").write_text(json.dumps(evidence, indent=1))
    which = "" if target == "local" else f" --release {release_dir}"
    say(f"Release written to {release_dir}. Review plan.json, then:\n"
        f"  scripts/local.sh lakehouse deploy -t {target}{which}")
    return 0


def _load(release_dir: Path, target: str) -> dict:
    evidence = json.loads((release_dir / "evidence.json").read_text())
    if evidence["target"] != target:
        raise ReleaseError(f"{release_dir.name} was released for {evidence['target']}, not {target}.")
    return evidence


def still_holds(evidence: dict, release_dir: Path, target: str) -> None:
    """Everything the evidence recorded, checked again now."""
    problems = []
    if git("rev-parse", "HEAD") != evidence["git"]["commit"]:
        problems.append(f"HEAD is no longer {evidence['git']['commit'][:12]}")
    if not evidence["git"]["dirty"] and git("status", "--porcelain", "--", *RELEASED_PATHS):
        problems.append("the released files changed since the release")
    wheel = DIST / evidence["wheel"]["name"]
    if not wheel.exists() or sha256(wheel) != evidence["wheel"]["sha256"]:
        problems.append(f"{wheel.relative_to(REPO)} is not the released wheel")
    if contract_fingerprint() != evidence["source_code_fingerprint"]:
        problems.append("the source's code fingerprint changed")
    fresh = cli_json(target, release_dir, "plan")
    reviewed = json.loads((release_dir / "plan.json").read_text())
    if fresh != reviewed:
        problems.append(f"a new plan differs from the reviewed one: now {resources(fresh)}, "
                        f"reviewed {resources(reviewed)}")
    if problems:
        raise ReleaseError("The release no longer matches what was reviewed; cut a new one:\n  " + "\n  ".join(problems))


def deploy(target: str, release_dir: Path) -> int:
    evidence = _load(release_dir, target)
    say(f"Deploying {evidence['release']} to {target}: checking that the evidence still holds...")
    still_holds(evidence, release_dir, target)
    say("  commit, wheel bytes, code fingerprint and plan are the reviewed ones")
    started = time.time()
    result = cli(target, release_dir, "deploy", "--plan", str(release_dir / "plan.json"), "--auto-approve", check=False)
    (release_dir / "deploy.log").write_text(result.stdout + result.stderr)
    record = {"started_at": datetime.fromtimestamp(started, timezone.utc).isoformat(),
              "seconds": round(time.time() - started, 1), "exit_code": result.returncode,
              "status": "deployed" if result.returncode == 0 else "failed"}
    (release_dir / "deploy.json").write_text(json.dumps(record, indent=1))
    if result.returncode:
        raise ReleaseError(
            f"The deployment failed part-way (see {release_dir / 'deploy.log'}). It is not retried: find out what "
            f"was applied (`databricks bundle summary -t {target}`, the workspace's job and its permissions) and "
            "why it failed, then cut a new release.")
    say(f"  deployed in {record['seconds']} s")
    # A new local lakehouse has no tables yet, and the health check reads them all. dev and prod
    # run `verify --setup` by hand, once, after their first deployment.
    return verify(target, release_dir, setup=target == "local")


def verify(target: str, release_dir: Path, setup: bool) -> int:
    evidence = _load(release_dir, target)
    reviewed = json.loads((release_dir / "plan.json").read_text())
    checks = {}

    replanned = cli_json(target, release_dir, "plan")
    drift = {key: action for key, action in resources(replanned).items() if action not in UNCHANGED}
    checks["no_drift"] = {"passed": not drift, "detail": drift or "a new plan finds nothing to change"}

    summary = cli_json(target, release_dir, "summary")
    job_id = summary["resources"]["jobs"][JOB]["id"]
    deployed = json.loads(run([CLI, "jobs", "get", str(job_id), "-o", "json"], env=cli_env(target, release_dir)).stdout)
    expected = job_settings(reviewed)
    different = sorted(key for key, value in expected.items() if deployed["settings"].get(key) != value)
    checks["job_matches_plan"] = {"passed": not different, "job_id": job_id,
                                  "detail": different or "every planned setting is the deployed one"}

    dependency = wheel_path(deployed["settings"])
    exported = release_dir / "deployed.whl"
    run([CLI, "workspace", "export", dependency, "--format", "AUTO", "--file", str(exported)], env=cli_env(target, release_dir))
    checks["deployed_wheel"] = {"passed": sha256(exported) == evidence["wheel"]["sha256"], "path": dependency,
                                "sha256": sha256(exported)}

    if setup:
        cli(target, release_dir, "run", JOB, "--params", "batch_id=__setup__")
    run_output = cli(target, release_dir, "run", JOB, "--params", "batch_id=__healthcheck__", check=False)
    (release_dir / "healthcheck.log").write_text(run_output.stdout + run_output.stderr)
    report = next((json.loads(line) for line in reversed((run_output.stdout + run_output.stderr).splitlines())
                   if line.startswith("{") and '"contract"' in line), None)
    checks["healthcheck"] = {
        "passed": bool(report) and report["contract"] == evidence["wheel"]["code_fingerprint"]
        and ".whl" in report.get("package", "") and all(report.get("tables", {}).values()),
        "detail": report or run_output.stderr[-500:],
    }
    passed = all(check["passed"] for check in checks.values())
    record = {"verified_at": datetime.now(timezone.utc).isoformat(), "passed": passed, "checks": checks}
    (release_dir / "verification.json").write_text(json.dumps(record, indent=1, default=str))
    for name, check in checks.items():
        say(f"  {'OK  ' if check['passed'] else 'FAIL'} {name}: {check.get('detail') if not check['passed'] else ''}".rstrip())
    say(f"Verification {'passed' if passed else 'FAILED'}: {release_dir / 'verification.json'}")
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("command", choices=["validate", "release", "deploy", "verify"])
    parser.add_argument("-t", "--target", required=True, choices=["local", "dev", "prod"])
    parser.add_argument("--release", type=Path,
                        help="the release folder (deploy, verify); for -t local, the newest release by default")
    parser.add_argument("--allow-dirty", action="store_true", help="local and dev only: release uncommitted changes")
    parser.add_argument("--skip-tests", action="store_true", help="not for prod")
    parser.add_argument("--setup", action="store_true", help="verify: create the shared tables first (__setup__)")
    parser.add_argument("--offline", action="store_true", help="release: everything but the plan (pull requests)")
    args = parser.parse_args(argv)
    try:
        if args.command == "validate":
            return validate(args.target)
        if args.command == "release":
            return release(args.target, args.allow_dirty, args.skip_tests, args.offline)
        if not args.release and args.target == "local":
            # Locally, the newest release; anywhere else the reviewed release is named, never guessed
            releases = sorted(p for p in RELEASES.glob("local-*") if (p / "evidence.json").exists())
            if not releases:
                parser.error("no local release yet: scripts/local.sh lakehouse release -t local")
            args.release = releases[-1]
            say(f"Using the newest local release, {args.release.name}")
        if not args.release:
            parser.error(f"{args.command} -t {args.target} needs --release <folder>: the release that was reviewed")
        if args.command == "deploy":
            return deploy(args.target, args.release.resolve())
        return verify(args.target, args.release.resolve(), args.setup)
    except ReleaseError as exc:
        say(f"STOPPED: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
