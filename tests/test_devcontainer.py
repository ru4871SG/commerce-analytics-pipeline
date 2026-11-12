"""The dev container - these tests hold the rules its files must keep."""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
FOLDER = REPO / ".devcontainer"
SCRIPT = (REPO / "scripts" / "local.sh").read_text()
DOCKERFILE = (FOLDER / "Dockerfile").read_text()


def config() -> dict:
    """devcontainer.json allows comments, which json does not: its comments are whole lines."""
    lines = (FOLDER / "devcontainer.json").read_text().splitlines()
    return json.loads("\n".join(line for line in lines if not line.lstrip().startswith("//")))


CONFIG = config()


def test_every_environment_is_installed_when_the_container_is_created():
    assert CONFIG["postCreateCommand"] == "scripts/local.sh install"
    assert "COPY --from=uv /uv /uvx /usr/local/bin/" in DOCKERFILE
    assert "COPY --from=java /opt/java/openjdk /opt/java/openjdk" in DOCKERFILE
    assert re.search(r"^ENV JAVA_HOME=/opt/java/openjdk", DOCKERFILE, re.MULTILINE)


def test_every_image_is_pinned_by_tag_and_digest():
    stages = re.findall(r"^FROM (\S+)", DOCKERFILE, re.MULTILINE)
    assert len(stages) == 3
    for image in stages:
        assert re.fullmatch(r"[a-z0-9./-]+:[A-Za-z0-9._-]+@sha256:[0-9a-f]{64}", image), image
    assert stages[-1].startswith("mcr.microsoft.com/devcontainers/base:") and "ubuntu24.04" in stages[-1]
    for unpinned in ("apt-get", "curl ", "wget ", "pip install"):
        assert unpinned not in DOCKERFILE, unpinned


def test_the_image_has_the_java_the_spark_engine_needs_and_the_uv_the_script_was_run_with():
    java = re.search(r"eclipse-temurin:(\d+)\.[\d._]+-jre-noble@", DOCKERFILE)
    assert int(java.group(1)) >= 17  
    assert re.search(r"ghcr\.io/astral-sh/uv:\d+\.\d+\.\d+@sha256:", DOCKERFILE)


def test_docker_runs_inside_the_container_from_one_exact_version_of_the_feature():
    assert list(CONFIG["features"]) == [next(iter(CONFIG["features"]))]
    (feature,) = CONFIG["features"]
    name, _, version = feature.rpartition(":")
    assert name == "ghcr.io/devcontainers/features/docker-in-docker"
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)
    lock = json.loads((FOLDER / "devcontainer-lock.json").read_text())["features"][feature]
    assert lock["version"] == version
    assert re.fullmatch(r"sha256:[0-9a-f]{64}", lock["integrity"])
    assert lock["resolved"] == f"{name}@{lock['integrity']}"
    engine = SCRIPT[SCRIPT.index("\nengine() {"):]
    assert "elif command -v docker >/dev/null; then echo docker" in engine[:engine.index("\n}\n")]


def ui_ports() -> dict[int, str]:
    """The port of each UI, as scripts/local.sh has it."""
    superset = int(re.search(r'^SUPERSET_PORT="\$\{SUPERSET_PORT:-(\d+)\}"', SCRIPT, re.MULTILINE).group(1))
    workspace = int(re.search(r"^LOCAL_WORKSPACE_PORT=(\d+)", SCRIPT, re.MULTILINE).group(1))
    airflow = int(re.search(r'^AIRFLOW_URL="http://127\.0\.0\.1:(\d+)"', SCRIPT, re.MULTILINE).group(1))
    dagster = int(re.search(r"--host 127\.0\.0\.1 --port (\d+)", SCRIPT).group(1))
    return {superset: "Superset", airflow: "Airflow", dagster: "Dagster", workspace: "Databricks"}


def test_every_ui_port_is_forwarded_with_a_label():
    ports = ui_ports()
    assert ports == {8088: "Superset", 8080: "Airflow", 3000: "Dagster", 8770: "Databricks"}
    assert sorted(CONFIG["forwardPorts"]) == sorted(ports)
    for port, name in ports.items():
        attributes = CONFIG["portsAttributes"][str(port)]
        assert name in attributes["label"], port
        assert attributes["onAutoForward"] == "notify"
    fakesnow = re.search(r'^FAKESNOW_PORT="\$\{FAKESNOW_PORT:-(\d+)\}"', SCRIPT, re.MULTILINE).group(1)
    for port in ("5433", fakesnow):
        assert CONFIG["portsAttributes"][port]["onAutoForward"] == "ignore"
        assert port not in map(str, CONFIG["forwardPorts"])
    assert CONFIG["otherPortsAttributes"] == {"onAutoForward": "ignore"}
    assert "AIRFLOW__API__HOST=127.0.0.1" in SCRIPT and "run -h 127.0.0.1 -p" in SCRIPT


def test_the_container_asks_for_a_machine_the_first_load_fits_on():
    assert CONFIG["hostRequirements"] == {"cpus": 4, "memory": "16gb", "storage": "32gb"}


def test_nothing_in_it_is_for_one_engine_or_one_person():
    text = (FOLDER / "devcontainer.json").read_text()
    for specific in ("runArgs", "userns", "podman", "mounts", "workspaceMount", "remoteUser", "/home/"):
        assert specific not in text, specific
    assert "containerUser" not in CONFIG and "privileged" not in CONFIG 
    assert CONFIG["containerEnv"] == {"CRM_SITE_BROWSER_DEPS": "true"}
    assert 'if [ "${CRM_SITE_BROWSER_DEPS:-}" = true ]; then' in SCRIPT
    for name in ("CRM_SITE_BROWSER_DEPS",):
        assert name not in (REPO / ".env.local.example").read_text()


def test_the_readme_s_two_ways_in_are_the_repository_s_own():
    readme = (REPO / "README.md").read_text()
    top = readme[:readme.index("![The end-to-end pipeline](docs/pipeline.svg)")]
    assert "See it without installing" in top
    assert "https://ru4871sg.github.io/commerce-analytics-pipeline/" in top
    assert "(https://codespaces.new/ru4871SG/commerce-analytics-pipeline)" in top
    assert "(.devcontainer/devcontainer.json)" in readme
