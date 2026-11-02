"""Secrets stay out of terminals and clear-text connections: dbt_env's inspection output and the
dashboard check's refusal to log in over plain http to another machine."""
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
