"""Secrets stay out of terminals and clear-text connections: dbt_env's inspection output and the
dashboard check's refusal to log in over plain http to another machine."""
import ast
import importlib.util
import sys
from pathlib import Path

import pytest

from crm_pipeline import dbt_env
from crm_pipeline.connections import Connection

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def snowflake_conn(monkeypatch):
    conn = Connection(
        conn_id="snowflake_default", login="AIRFLOW_SVC", password="pa55word",
        extra={"account": "myorg-myaccount", "private_key_content": "-----BEGIN PRIVATE KEY-----",
               "private_key_passphrase": "phrase"},
    )
    monkeypatch.setattr(dbt_env, "get_connection", lambda conn_id: conn)
    return conn


def test_printing_the_dbt_environment_redacts_the_secrets(snowflake_conn, capsys):
    assert dbt_env.main([]) == 0
    printed = capsys.readouterr().out
    for secret in ("pa55word", "BEGIN PRIVATE KEY", "phrase"):
        assert secret not in printed
    assert "SNOWFLAKE_PASSWORD=<redacted>" in printed
    assert "SNOWFLAKE_USER=AIRFLOW_SVC" in printed
    assert "SNOWFLAKE_ACCOUNT=myorg-myaccount" in printed


def test_an_unset_secret_prints_as_empty_not_redacted(monkeypatch, capsys):
    monkeypatch.setattr(dbt_env, "get_connection", lambda conn_id: Connection(conn_id=conn_id, login="u"))
    dbt_env.main([])
    assert "SNOWFLAKE_PRIVATE_KEY=\n" in capsys.readouterr().out


def test_the_shell_exports_keep_the_real_values(snowflake_conn, tmp_path):
    out = tmp_path / "exports.sh"
    assert dbt_env.main(["--shell", str(out)]) == 0
    assert "export SNOWFLAKE_PASSWORD=pa55word\n" in out.read_text()


@pytest.fixture(scope="module")
def check_dashboard():
    folder = REPO_ROOT / "local" / "superset"
    spec = importlib.util.spec_from_file_location("check_dashboard", folder / "check_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)      # puts its own folder on sys.path to import build_bundle
    yield module
    sys.modules.pop("build_bundle", None)
    if str(folder) in sys.path:
        sys.path.remove(str(folder))


@pytest.fixture(scope="module")
def capture_dashboard():
    before = list(sys.path)
    spec = importlib.util.spec_from_file_location(
        "capture_dashboard", REPO_ROOT / "local" / "site" / "capture_dashboard.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)      # puts local/superset on sys.path to import safe_url
    yield module
    sys.path[:] = before


@pytest.mark.parametrize("url, refused", [
    ("http://127.0.0.1:8088", False),
    ("http://localhost:8088", False),
    ("http://[::1]:8088", False),
    ("http://127.0.0.2:8088", False),
    ("https://superset.example.com", False),
    ("http://superset.example.com", True),
    ("http://10.0.0.5:8088", True),
    ("http://0.0.0.0:8088", True),
])
def test_the_dashboard_check_never_sends_its_password_in_clear(check_dashboard, url, refused):
    assert check_dashboard.sends_password_in_clear(url) is refused


class Answer:
    def __init__(self, status_code):
        self.status_code = status_code

    def raise_for_status(self):
        raise AssertionError("the login refusal was not handled")


def test_without_a_password_the_dashboard_check_says_so(check_dashboard, monkeypatch, capsys):
    monkeypatch.delenv("SUPERSET_PASSWORD", raising=False)
    monkeypatch.setattr(check_dashboard.requests.Session, "get", lambda self, url, timeout: Answer(200))
    assert check_dashboard.main() == check_dashboard.LOGIN_FAILED
    assert "SUPERSET_PASSWORD is not set" in capsys.readouterr().err


def test_without_a_password_a_stopped_superset_is_still_skipped(check_dashboard, monkeypatch):
    monkeypatch.delenv("SUPERSET_PASSWORD", raising=False)

    def refused(self, url, timeout):
        raise check_dashboard.requests.ConnectionError()

    monkeypatch.setattr(check_dashboard.requests.Session, "get", refused)
    assert check_dashboard.main() == check_dashboard.NOT_RUNNING


def test_a_refused_login_is_reported_not_raised(check_dashboard, monkeypatch, capsys):
    monkeypatch.setenv("SUPERSET_PASSWORD", "wrong")
    monkeypatch.setattr(check_dashboard.requests.Session, "post", lambda self, url, json, timeout: Answer(401))
    assert check_dashboard.main() == check_dashboard.LOGIN_FAILED
    assert "refused the login of admin" in capsys.readouterr().err


def test_the_dashboard_check_refuses_before_logging_in(check_dashboard, monkeypatch):
    monkeypatch.setattr(check_dashboard, "BASE_URL", "http://superset.example.com")

    def no_login(*args, **kwargs):
        raise AssertionError("logged in over http")

    monkeypatch.setattr(check_dashboard.requests.Session, "post", no_login)
    assert check_dashboard.main() == check_dashboard.INSECURE_URL


def test_the_change_data_capture_never_handles_its_password_nor_formats_a_message():
    tree = ast.parse((REPO_ROOT / "dags" / "crm_pipeline" / "cdc.py").read_text())
    names = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert not names & {"password", "login", "get_connection", "extra"}
    shown = set()

    def names(node):
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "len":
            return                  # how many rows a read held says nothing of what is in them
        if isinstance(node, ast.Name):
            shown.add(node.id)
        for child in ast.iter_child_nodes(node):
            names(child)

    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and ast.unparse(node.func) in ("log.info", "log.error", "log.warning", "PermanentError",
                                                                    "RuntimeError", "print"):
            for part in [*node.args, *node.keywords]:
                names(part)
    assert not shown & {"data", "message", "values", "rows", "row", "sent", "change", "changes", "params", "sql"}
    assert {"brand", "slot", "lsn"} <= shown           # what they do say: which brand, which slot, where in the log



def test_the_check_and_the_screenshot_share_one_rule_for_clear_text(check_dashboard, capture_dashboard):
    assert check_dashboard.sends_password_in_clear.__module__ == "safe_url"
    assert capture_dashboard.sends_password_in_clear.__module__ == "safe_url"


def no_superset():
    raise AssertionError("contacted Superset")


def test_the_screenshot_refuses_a_clear_text_url_before_it_contacts_superset(capture_dashboard, monkeypatch, tmp_path):
    monkeypatch.setattr(capture_dashboard, "BASE_URL", "http://superset.example.com")
    monkeypatch.setattr(capture_dashboard, "is_running", no_superset)
    monkeypatch.setenv("SUPERSET_PASSWORD", "s3cret-value")
    assert capture_dashboard.main([str(tmp_path / "dashboard.png")]) == capture_dashboard.INSECURE_URL
    assert not (tmp_path / "dashboard.png").exists()


def test_without_a_password_the_screenshot_says_so_and_opens_no_browser(capture_dashboard, monkeypatch, tmp_path, capsys):
    monkeypatch.delenv("SUPERSET_PASSWORD", raising=False)
    monkeypatch.setattr(capture_dashboard, "is_running", lambda: True)
    assert capture_dashboard.main([str(tmp_path / "dashboard.png")]) == capture_dashboard.LOGIN_FAILED
    assert "SUPERSET_PASSWORD is not set" in capsys.readouterr().err


def test_a_stopped_superset_skips_the_screenshot_as_it_skips_the_check(capture_dashboard, monkeypatch, tmp_path):
    monkeypatch.setenv("SUPERSET_PASSWORD", "s3cret-value")
    monkeypatch.setattr(capture_dashboard, "is_running", lambda: False)
    assert capture_dashboard.main([str(tmp_path / "dashboard.png")]) == capture_dashboard.NOT_RUNNING


class LoginPage:
    """Superset's login page, as far as log_in uses it: it never lets this session in."""

    url = "http://127.0.0.1:8088/login/"

    def __init__(self):
        self.typed = {}

    def goto(self, url, wait_until):
        assert url == "http://127.0.0.1:8088/login/"

    def fill(self, selector, value):
        self.typed[selector] = value

    def click(self, selector):
        assert selector == "[data-test=login-button]"

    def wait_for_timeout(self, milliseconds):
        pass


def test_the_password_goes_into_the_password_field_and_nowhere_else(capture_dashboard, monkeypatch, capsys):
    monkeypatch.setattr(capture_dashboard, "BASE_URL", "http://127.0.0.1:8088")
    page = LoginPage()
    assert capture_dashboard.log_in(page, "admin", "s3cret-value", seconds=0.05) is False
    assert page.typed == {"#username": "admin", "#password": "s3cret-value"}
    printed = capsys.readouterr()
    assert "s3cret-value" not in printed.out + printed.err


def test_the_screenshot_script_uses_the_password_only_to_fill_the_login_form():
    tree = ast.parse((REPO_ROOT / "local" / "site" / "capture_dashboard.py").read_text())
    uses = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            inside = {name.id for part in [*node.args, *node.keywords] for name in ast.walk(part)
                      if isinstance(name, ast.Name)}
            if "password" in inside:
                uses.append(ast.unparse(node.func))
    assert sorted(uses) == ["log_in", "page.fill"]
