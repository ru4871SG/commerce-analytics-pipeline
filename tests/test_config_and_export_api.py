"""The brand registry, the connection parser, and how the export API's answers turn into outcomes."""
from types import SimpleNamespace

import pytest

from crm_pipeline import config, connections, export_api
from crm_pipeline.errors import PermanentError


def test_brands_come_from_the_dbt_seed():
    brands = {b.key: b for b in config.load_brands()}
    assert set(brands) == {"broomx", "wecare", "greenz"}
    assert brands["broomx"].name == "Broomx"
    assert brands["broomx"].source_database == "crm_broomx"
    assert brands["wecare"].currency_code == "GBP"


def test_brands_not_loaded_from_source_are_not_extracted(tmp_path):
    seed = tmp_path / "brands.csv"
    seed.write_text(
        "brand,brand_name,source_database,load_from_source,currency_code,prices_include_vat,vat_rate\n"
        "broomx,Broomx,crm_broomx,true,USD,false,0.00\n"
        "traderjoe,Trader Joe,,false,USD,true,0.20\n"
    )
    assert [b.key for b in config.brands_to_extract(seed)] == ["broomx"]
    assert [b.key for b in config.load_brands(seed)] == ["broomx", "traderjoe"]


@pytest.fixture
def export_conn(monkeypatch):
    conn = SimpleNamespace(
        password="s3cret",
        extra={"base_url": "http://export.example.com:82/", "secret_header": "x-export-secret"},
    )
    monkeypatch.setattr(export_api.connections, "get_connection", lambda conn_id: conn)
    return conn


def fake_post(monkeypatch, status, body=None):
    calls = []

    def post(url, headers, data, timeout):
        calls.append((url, headers, data))
        return SimpleNamespace(status_code=status, text=str(body), json=lambda: body,
                               raise_for_status=lambda: None)

    monkeypatch.setattr(export_api.requests, "post", post)
    return calls


def test_export_sends_the_brand_and_the_secret(monkeypatch, export_conn):
    calls = fake_post(monkeypatch, 200, {"status": "done"})
    assert export_api.trigger_crm_export("broomx") == {"status": "done"}
    url, headers, data = calls[0]
    assert url == "http://export.example.com:82/api/export-crm"
    assert headers["x-export-secret"] == "s3cret"
    assert data == {"brand": "broomx"}


def test_an_export_already_running_is_retried(monkeypatch, export_conn):
    fake_post(monkeypatch, 429, "Process already running")
    with pytest.raises(export_api.ExportAlreadyRunning):
        export_api.trigger_crm_export("broomx")


def test_a_refused_export_fails_without_retrying(monkeypatch, export_conn):
    fake_post(monkeypatch, 403, "bad secret")
    with pytest.raises(PermanentError):
        export_api.trigger_crm_export("broomx")


def test_no_export_service_means_the_step_is_skipped(monkeypatch):
    def missing(conn_id):
        raise connections.ConnectionNotFound(f"The conn_id `{conn_id}` isn't defined")

    monkeypatch.setattr(export_api.connections, "get_connection", missing)
    assert export_api.trigger_crm_export("broomx") == {"skipped": True}


def test_a_broken_secrets_backend_fails_instead_of_skipping(monkeypatch):
    def unreachable(conn_id):
        raise RuntimeError("secrets backend timed out")

    monkeypatch.setattr(export_api.connections, "get_connection", unreachable)
    with pytest.raises(RuntimeError):
        export_api.trigger_crm_export("broomx")


def test_uri_connection_parser_keeps_the_query_in_extra():
    conn = connections.parse_connection(
        "crm_source_postgresql",
        "postgresql://read%40only:p%40ss%20word@crm-db.example.com:5432?sslmode=require&application_name=crm",
    )
    assert (conn.conn_type, conn.host, conn.port) == ("postgresql", "crm-db.example.com", 5432)
    assert (conn.login, conn.password) == ("read@only", "p@ss word")
    assert conn.extra == {"sslmode": "require", "application_name": "crm"}


def test_json_connection_parser_reads_extra_and_top_level_keys():
    conn = connections.parse_connection(
        "snowflake_default",
        '{"conn_type": "snowflake", "login": "svc", "password": "pw", '
        '"extra": {"account": "org", "host": "127.0.0.1", "port": 64616, "protocol": "http"}}',
    )
    assert conn.conn_type == "snowflake" and conn.login == "svc"
    assert conn.extra["host"] == "127.0.0.1" and conn.extra["port"] == 64616
    assert conn.extra_dejson is conn.extra


def test_a_missing_environment_connection_raises_connection_not_found(monkeypatch):
    monkeypatch.delenv("AIRFLOW_CONN_MISSING", raising=False)
    with pytest.raises(connections.ConnectionNotFound):
        connections.get_connection("missing")


def test_permanent_error_is_an_airflow_fail_exception_under_airflow():
    pytest.importorskip("airflow")
    from airflow.sdk.exceptions import AirflowFailException

    assert issubclass(PermanentError, AirflowFailException)


def _env_connections(path):
    """The AIRFLOW_CONN_* lines of an env file."""
    values = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line.startswith("AIRFLOW_CONN_") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key] = value
    return values


@pytest.mark.parametrize("filename", [".env.local.example", ".env.example"])
def test_environment_parser_matches_airflow(monkeypatch, filename):
    pytest.importorskip("airflow")
    from airflow.models.connection import Connection as AirflowConnection

    from pathlib import Path

    path = Path(__file__).resolve().parents[1] / filename
    for env_name, raw in _env_connections(path).items():
        conn_id = env_name.removeprefix("AIRFLOW_CONN_").lower()
        monkeypatch.setenv(env_name, raw)
        ours = connections.parse_connection(conn_id, raw)
        theirs = AirflowConnection.get_connection_from_secrets(conn_id)
        assert (ours.host, ours.port, ours.login, ours.password) == (
            theirs.host, theirs.port, theirs.login, theirs.password
        ), conn_id
        assert ours.extra == dict(theirs.extra_dejson), conn_id
