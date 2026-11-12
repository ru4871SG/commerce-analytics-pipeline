"""The clickstream from the orchestrators' side: the step after a plan, the retry that moves a
batch to Spark, publishing gold to the warehouse, and the Databricks adapters (job runs, vended
credentials, the landing volume) against fake clients. No database, no Spark."""
from __future__ import annotations

import io
import json
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

pytest.importorskip("deltalake")

from crm_lakehouse import delta_io, planning, runner, unity_catalog  # noqa: E402
from crm_lakehouse.contract import GOLD_FUNNEL  # noqa: E402
from crm_lakehouse.layout import Lakehouse  # noqa: E402

from crm_pipeline import lakehouse  # noqa: E402


@pytest.fixture
def local(tmp_path, monkeypatch):
    """A local lakehouse in a temporary folder, as the orchestrators open it."""
    house = Lakehouse("test", root=str(tmp_path / "lakehouse"))
    monkeypatch.setattr(lakehouse, "open_lakehouse", lambda: (house, None))
    return house


@pytest.mark.parametrize("plan, step", [
    ({"action": "run", "engine": "pandas"}, "run_in_process"),
    ({"action": "run", "engine": "spark"}, "run_on_spark"),
    ({"action": "publish", "engine": "spark"}, "publish_web_funnel"),
    ({"action": "nothing", "engine": None}, "publish_web_funnel"),
])
def test_the_step_after_the_plan(plan, step):
    assert lakehouse.next_step(plan) == step


def test_without_a_databricks_connection_the_lakehouse_is_local(monkeypatch):
    def missing(conn_id):
        raise lakehouse.connections.ConnectionNotFound(conn_id)

    monkeypatch.setattr(lakehouse.connections, "get_connection", missing)
    house, client = lakehouse.open_lakehouse()
    assert client is None and house.is_local and house.catalog == lakehouse.LAKEHOUSE_CATALOG


def test_a_retried_in_process_run_moves_the_batch_to_spark(local, monkeypatch):
    path = f"{local.landing}/2026-10-27/a.json.gz"
    import os

    os.makedirs(os.path.dirname(path))
    with open(path, "wb") as out:
        out.write(b"x" * 100)
    first = lakehouse.plan(None, "airflow", "run-1")
    assert first["engine"] == "pandas"
    planning.record(local, first["batch_id"], "RUNNING", engine="pandas")     # then the worker died
    sent = {}
    monkeypatch.setattr(lakehouse, "run_on_spark", lambda plan, *args, **kwargs: sent.setdefault("plan", plan))
    lakehouse.run_in_process(first, "airflow", "run-1", try_number=2, echo=lambda line: None)
    assert sent["plan"]["batch_id"] == first["batch_id"] and sent["plan"]["engine"] == "spark"
    assert sent["plan"]["reason"].startswith("escalated")


class RecordingCursor:
    """Records every statement. A lookup of batch ids finds those in held, the batches the
    warehouse's RAW.WEB.EVENT_BATCHES holds."""

    def __init__(self, held=()):
        self.statements, self.rows, self.held, self.found = [], [], set(held), []

    def execute(self, sql, params=None):
        self.statements.append(" ".join(sql.split()))
        if sql.startswith("select BATCH_ID"):
            self.found = [(batch_id,) for batch_id in params if batch_id in self.held]

    def executemany(self, sql, rows):
        self.statements.append(" ".join(sql.split()))
        self.rows += rows

    def fetchall(self):
        return self.found

    def close(self):
        pass


class RecordingConnection:
    def __init__(self, held=()):
        self.cursors, self.held = [], held

    def cursor(self):
        self.cursors.append(RecordingCursor(self.held))
        return self.cursors[-1]


def succeeded_batch(house) -> str:
    planning.record(house, "b1", "PLANNED", engine="pandas", reason="fits", files_json="[]", files=1,
                    compressed_bytes=10, budget_bytes=1)
    gold = [{"brand": "broomx", "business_date": date(2026, 10, day), "channel": "direct", "device": "mobile",
             "sessions": 2, "engaged_sessions": 1, "view_item_sessions": 1, "add_to_cart_sessions": 0,
             "checkout_sessions": 0, "purchase_sessions": 0, "purchases": 0, "events": 9, "_batch_id": "b1",
             "_engine": "pandas"} for day in (26, 27)]
    import pandas as pd

    delta_io.replace_days(house, GOLD_FUNNEL, delta_io.to_arrow(pd.DataFrame(gold), GOLD_FUNNEL),
                          date(2026, 10, 26), date(2026, 10, 27))
    planning.record(house, "b1", "SUCCEEDED", engine="pandas", bronze_rows=18, gold_from=date(2026, 10, 26),
                    gold_to=date(2026, 10, 27), gold_rows=2, dropped_json=json.dumps({"parsable": 1}))
    return "b1"


def test_publishing_replaces_the_batch_days_in_one_transaction_then_records_it(local):
    batch = succeeded_batch(local)
    connection = RecordingConnection()
    published = lakehouse.publish({"action": "run", "batch_id": batch, "reason": ""}, connection)
    assert published["published"] and published["gold_rows"] == 2 and published["dropped"] == {"parsable": 1}
    statements = connection.cursors[0].statements
    begin, commit = statements.index("begin"), statements.index("commit")
    inside = statements[begin + 1:commit]
    assert inside[0].startswith('delete from RAW.WEB."DAILY_FUNNEL" where BUSINESS_DATE between')
    assert inside[1].startswith('insert into RAW.WEB."DAILY_FUNNEL"')
    assert inside[2].startswith('delete from RAW.WEB."EVENT_BATCHES" where BATCH_ID')
    assert len(connection.cursors[0].rows) == 3                       # two gold rows, one batch row
    assert planning.latest_states(planning.ledger(local))[batch]["state"] == "PUBLISHED"
    # Published once: a second call, to a warehouse that holds the batch, has nothing to do
    again = RecordingConnection(held={batch})
    assert lakehouse.publish({"action": "publish", "batch_id": batch, "reason": ""}, again) == {
        "published": False, "reason": f"{batch} was already published", "republished": []}
    assert "begin" not in again.cursors[0].statements


def test_published_batches_the_warehouse_lost_are_copied_again(local):
    """A warehouse reset empties RAW.WEB, but the ledger still says the batch was published: the
    planner has nothing to send, so the next publish copies the batch again."""
    batch = succeeded_batch(local)
    lakehouse.publish({"action": "run", "batch_id": batch, "reason": ""}, RecordingConnection())
    ledger_rows = len(planning.ledger(local))
    reset = RecordingConnection()                                     # RAW.WEB holds no batch
    published = lakehouse.publish({"action": "nothing", "batch_id": None, "reason": "no file"}, reset)
    assert not published["published"] and published["republished"] == [batch]
    statements = reset.cursors[0].statements
    inside = statements[statements.index("begin") + 1:statements.index("commit")]
    assert inside[0].startswith('delete from RAW.WEB."DAILY_FUNNEL" where BUSINESS_DATE between')
    assert len(reset.cursors[0].rows) == 3                            # its two gold rows, its batch row
    assert len(planning.ledger(local)) == ledger_rows                 # a warehouse repair: the ledger is unchanged


def test_nothing_is_copied_again_when_the_warehouse_holds_every_published_batch(local):
    batch = succeeded_batch(local)
    lakehouse.publish({"action": "run", "batch_id": batch, "reason": ""}, RecordingConnection())
    intact = RecordingConnection(held={batch})
    published = lakehouse.publish({"action": "nothing", "batch_id": None, "reason": "no file"}, intact)
    assert published["republished"] == [] and "begin" not in intact.cursors[0].statements


def test_nothing_to_publish_still_creates_the_warehouse_tables(local):
    connection = RecordingConnection()
    assert not lakehouse.publish({"action": "nothing", "batch_id": None, "reason": "no file"}, connection)["published"]
    created = connection.cursors[0].statements
    assert any("create table if not exists RAW.WEB.\"DAILY_FUNNEL\"" in s for s in created)


def test_the_dbt_step_creates_the_same_two_tables_so_a_first_batch_that_fails_leaves_dbt_its_sources(local):
    """dbt does not wait for the clickstream, and its sources are these tables: without them
    `dbt source freshness` fails and the report is not built. A batch that fails never reaches
    publish, so publishing cannot be the only step that creates them."""
    from crm_pipeline import raw_tables

    by_dbt, by_publish = RecordingCursor(), RecordingConnection()
    raw_tables.create_side_tables(by_dbt)
    lakehouse.publish({"action": "nothing", "batch_id": None, "reason": "no file"}, by_publish)
    web = [statement for statement in by_dbt.statements if "RAW.WEB" in statement]
    assert web == [statement for statement in by_publish.cursors[0].statements if statement.startswith("create")
                   and "RAW.WEB" in statement]
    # The statements as they were when only publish ran them: a warehouse that has the tables keeps them
    assert web == [
        "create schema if not exists RAW.WEB",
        'create table if not exists RAW.WEB."DAILY_FUNNEL" ("BRAND" VARCHAR, "BUSINESS_DATE" DATE, "CHANNEL" VARCHAR, '
        '"DEVICE" VARCHAR, "SESSIONS" NUMBER(38, 0), "ENGAGED_SESSIONS" NUMBER(38, 0), "VIEW_ITEM_SESSIONS" NUMBER(38, 0), '
        '"ADD_TO_CART_SESSIONS" NUMBER(38, 0), "CHECKOUT_SESSIONS" NUMBER(38, 0), "PURCHASE_SESSIONS" NUMBER(38, 0), '
        '"PURCHASES" NUMBER(38, 0), "EVENTS" NUMBER(38, 0), "_BATCH_ID" VARCHAR, "_ENGINE" VARCHAR, '
        "_LOADED_AT TIMESTAMP_NTZ)",
        'create table if not exists RAW.WEB."EVENT_BATCHES" ("BATCH_ID" VARCHAR, "ENGINE" VARCHAR, "REASON" VARCHAR, '
        '"FILES" NUMBER(38, 0), "COMPRESSED_BYTES" NUMBER(38, 0), "ESTIMATED_ROWS" NUMBER(38, 0), '
        '"ESTIMATED_PANDAS_BYTES" NUMBER(38, 0), "BUDGET_BYTES" NUMBER(38, 0), "BRONZE_ROWS" NUMBER(38, 0), '
        '"SILVER_INSERTED" NUMBER(38, 0), "DROPPED_JSON" VARCHAR, "GOLD_FROM" DATE, "GOLD_TO" DATE, '
        '"GOLD_ROWS" NUMBER(38, 0), "OBSERVED_PANDAS_BYTES" NUMBER(38, 0), "SECONDS" FLOAT, "CONTRACT" VARCHAR, '
        '"ORCHESTRATOR" VARCHAR, "RUN_ID" VARCHAR, "PLANNED_AT" TIMESTAMP_NTZ, "SUCCEEDED_AT" TIMESTAMP_NTZ, '
        "_LOADED_AT TIMESTAMP_NTZ)",
    ]
    # Every source of dbt that a step outside the report writes is among them (dbt/models/bronze/_sources.yml)
    assert {table.split(".", 1)[1].replace('"', "") for table in raw_tables.create_side_tables(RecordingCursor())} == {
        "REF.FX_RATES", "CRM.ORDERS_CDC", "WEB.DAILY_FUNNEL", "WEB.EVENT_BATCHES"}


def test_a_batch_that_did_not_succeed_is_not_published(local):
    planning.record(local, "b2", "PLANNED", engine="spark", files_json="[]")
    planning.record(local, "b2", "FAILED", engine="spark", error="boom")
    with pytest.raises(lakehouse.PermanentError):
        lakehouse.publish({"action": "run", "batch_id": "b2", "reason": ""}, RecordingConnection())


# --- Databricks, against fakes -------------------------------------------------------------

class FakeJobs:
    def __init__(self, jobs, result_state="SUCCESS"):
        self.jobs, self.result_state, self.triggered = jobs, result_state, []

    def list(self, name):
        return [job for job in self.jobs if job.settings.name == name]

    def run_now(self, job_id, job_parameters):
        from databricks.sdk.service.jobs import RunResultState

        self.triggered.append((job_id, job_parameters))
        run = SimpleNamespace(run_id=7, run_page_url="https://example/run/7",
                              state=SimpleNamespace(result_state=RunResultState(self.result_state), state_message="msg"))
        return SimpleNamespace(response=SimpleNamespace(run_id=7), result=lambda timeout: run)


def job(job_id, name="web_events_spark"):
    return SimpleNamespace(job_id=job_id, settings=SimpleNamespace(name=name))


def test_the_spark_job_is_triggered_with_the_batch_and_waited_for():
    pytest.importorskip("databricks.sdk")
    client = SimpleNamespace(jobs=FakeJobs([job(42)]))
    url = runner.run_on_databricks(client, "web_events_spark", "b1", echo=lambda line: None)
    assert url == "https://example/run/7" and client.jobs.triggered == [(42, {"batch_id": "b1"})]


def test_a_missing_or_failed_job_run_fails_the_task():
    pytest.importorskip("databricks.sdk")
    with pytest.raises(runner.SparkJobFailed, match="deploy the bundle"):
        runner.run_on_databricks(SimpleNamespace(jobs=FakeJobs([])), "web_events_spark", "b1", echo=lambda line: None)
    failing = SimpleNamespace(jobs=FakeJobs([job(1)], result_state="FAILED"))
    with pytest.raises(runner.SparkJobFailed, match="ended"):
        runner.run_on_databricks(failing, "web_events_spark", "b1", echo=lambda line: None)


def test_vended_credentials_are_scoped_to_the_table_and_reused_until_they_near_expiry():
    pytest.importorskip("databricks.sdk")
    calls = []
    soon = datetime.now(timezone.utc) + timedelta(minutes=5)
    later = datetime.now(timezone.utc) + timedelta(hours=1)
    expirations = iter([later, soon, later])

    def generate(table_id, operation):
        calls.append((table_id, operation.value))
        return SimpleNamespace(
            expiration_time=int(next(expirations).timestamp() * 1000),
            aws_temp_credentials=SimpleNamespace(access_key_id="AK", secret_access_key="SK", session_token="ST"))

    client = SimpleNamespace(
        tables=SimpleNamespace(get=lambda name: SimpleNamespace(table_id=f"id-{name}")),
        temporary_table_credentials=SimpleNamespace(generate_temporary_table_credentials=generate))
    credentials = unity_catalog.vended_credentials(client, region="us-east-1")
    options = credentials("prod.silver.web_events", "READ_WRITE")
    assert options == {"AWS_ACCESS_KEY_ID": "AK", "AWS_SECRET_ACCESS_KEY": "SK", "AWS_SESSION_TOKEN": "ST",
                       "conditional_put": "etag", "AWS_REGION": "us-east-1"}
    credentials("prod.silver.web_events", "READ_WRITE")              # an hour left: reused
    credentials("prod.gold.web_daily_funnel", "READ")                # another table: its own
    credentials("prod.gold.web_daily_funnel", "READ")                # five minutes left: renewed
    assert calls == [("id-prod.silver.web_events", "READ_WRITE"), ("id-prod.gold.web_daily_funnel", "READ"),
                     ("id-prod.gold.web_daily_funnel", "READ")]


def test_the_landing_volume_is_listed_and_read_through_the_files_api():
    tree = {
        "/Volumes/prod/bronze/landing/web_events": [("2026-10-27", True, None), ("_tmp", True, None)],
        "/Volumes/prod/bronze/landing/web_events/2026-10-27": [("broomx-2026-10-27.json.gz", False, 12),
                                                              (".partial.json.gz", False, 3)],
    }
    entries = lambda folder: [SimpleNamespace(path=f"{folder}/{name}", is_directory=d, file_size=size)  # noqa: E731
                              for name, d, size in tree.get(folder, [])]
    client = SimpleNamespace(files=SimpleNamespace(
        list_directory_contents=entries,
        download=lambda path: SimpleNamespace(contents=io.BytesIO(b"gz bytes"))))
    files = unity_catalog.VolumeFiles(client)
    assert files.list("/Volumes/prod/bronze/landing/web_events") == [
        ("/Volumes/prod/bronze/landing/web_events/2026-10-27/broomx-2026-10-27.json.gz", 12)]
    assert files.open("any").read() == b"gz bytes"
