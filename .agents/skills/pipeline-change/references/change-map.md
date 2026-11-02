# Change map: what lives where, and what moves together

Use this map in steps 1 and 3 of the workflow, to find every file a change touches. Paths are from
the repo root.

## The parts of the pipeline

| Part                                    | Code                                                                                                                                                                                        | Airflow                                     | Dagster                                      | Tests                                                                                            | README     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------- |
| Demo shop simulator, clickstream export | `dags/crm_pipeline/simulator.py`, `dags/crm_pipeline/clickstream.py`                                                                                                                        | `dags/crm_daily_activity.py`                | `dagster_crm/activity.py`                    | `tests/test_simulator.py`, `tests/test_clickstream.py`                                           | §4         |
| Source tables, the brand registry       | `dags/crm_pipeline/config.py` (`SOURCE_TABLES`), `dbt/seeds/brands.csv`, `local/source_db/generate_fake_crm.py`                                                                             | every DAG                                   | every module                                 | `tests/test_config_and_export_api.py`                                                            | §1, §2     |
| CRM ingest into bronze                  | `source_postgres.py`, `bronze_loader.py`, `bronze_schema.py`, `export_api.py`                                                                                                               | `crm_executive_report.py`: `ingest_<brand>` | `dagster_crm/bronze.py`: `bronze_ingest`     | `tests/test_bronze_loader.py`, `tests/test_source_postgres.py`                                   | §2         |
| Streams and merges                      | `stream_task.py`; the real objects in `snowflake/crm_streams_and_tasks.sql`                                                                                                                 | `streams_and_tasks` group                   | `bronze_merge`                               | `tests/test_stream_task.py`                                                                      | §2         |
| Reconciliation and resync               | `bronze_loader.reconcile`                                                                                                                                                                   | `reconcile_bronze`                          | `reconcile_<table>` checks in `bronze_merge` | `tests/test_bronze_loader.py`                                                                    | §2         |
| Clickstream lakehouse                   | `dags/crm_pipeline/lakehouse.py`, `lakehouse/src/crm_lakehouse/*`                                                                                                                           | `lakehouse` group                           | `dagster_crm/lakehouse.py`: `web_events`     | `lakehouse/tests/` (parity), `tests/test_lakehouse_pipeline.py`, `tests/test_local_workspace.py` | §2, §8     |
| dbt silver and gold                     | `dbt/`                                                                                                                                                                                      | `dags/crm_pipeline/dbt.py` (`dbt_build`)    | `dagster_crm/dbt_assets.py`                  | dbt's own tests (in `dbt build`), `tests/test_dag_integrity.py`                                  | §2, §3, §6 |
| Activation                              | `digest.py`, `notifications.py`, `local/superset/check_dashboard.py`                                                                                                                        | `crm_executive_activation.py`               | `dagster_crm/activation.py`                  | `tests/test_digest.py`                                                                           | §4         |
| Dashboard                               | `local/superset/build_bundle.py`, `import_dashboard.py`, `superset_config.py`                                                                                                               | (none)                                      | (none)                                       | `check_dashboard.py`                                                                             | §3, §4     |
| Lakehouse deployment                    | `lakehouse/databricks.yml`, `lakehouse/resources/`, `scripts/lakehouse_release.py`, `local/databricks_workspace.py`, `.github/workflows/lakehouse.yml`, `lakehouse/setup/unity_catalog.sql` | (none)                                      | (none)                                       | release checks, `tests/test_local_workspace.py`                                                  | §4, §8     |
| Local entry point                       | `scripts/local.sh`, `requirements.txt`, `.env.local.example`, `.gitignore`                                                                                                                  | (all)                                       | (all)                                        | `scripts/local.sh test`                                                                          | §4         |

The `dagster_crm/` modules are under `dagster/dagster_crm/`; the helpers are under
`dags/crm_pipeline/`. Every Dagster job and schedule is assembled in
`dagster_crm/definitions.py` and `dagster_crm/automation.py`. The report job's selection is the
asset groups `bronze`, `lakehouse` and `dbt`.

## Adding a tool, engine or source: everything that moves together

1. **Environment.**
   - Add a `# [name]` section to `requirements.txt`, with exact pins.
   - Add `make_venv "$REPO/.venv-<name>" <name>` to `install` in `scripts/local.sh`.
   - Add `.venv-<name>/` to `.gitignore`.
   - If it has tests, add its run to the `test` command.
   - Add any new command to README §4's command lists, and to the `usage:` line of its group in
     `scripts/local.sh`. The script has no help text of its own: without a command, it points to
     the README.
   - Update the "Needs" line in README §4. A JVM, a CLI or a container image counts as a need.
   - A downloaded binary is pinned with its SHA-256, as `databricks_cli()` does.
2. **Helper module.** Put it in `dags/crm_pipeline/` (or `crm_lakehouse`), with no Airflow import,
   a docstring that says why, and unit tests that run in `.venv` and `.venv-dagster`.
3. **Orchestrator wiring.** Add the Airflow task or group and the Dagster op or asset, both
   calling the helper.
   - Retries: Airflow `retries`; Dagster `RetryPolicy` through `dagster_crm/retries.py`.
   - A permanent failure raises `crm_pipeline.errors.PermanentError`, which Dagster turns into a
     `Failure` that is not retried.
   - Shape tests go in `tests/test_dag_integrity.py` and `tests/dagster/test_definitions.py`.
4. **Connections.**
   - Add a constant in `config.py`.
   - A local value goes in `.env.local.example`, without real secrets; the real form goes in
     `.env.example`.
   - Document any real-system connection settings in README §7.
   - An optional connection must skip cleanly when it is absent, as `crm_export_api` and
     `databricks_default` do.
5. **Warehouse objects.**
   - Whatever writes a table creates it idempotently (`create ... if not exists`), as
     `ensure_web_tables` does.
   - Each new raw table becomes a dbt `source`, with a description and a deliberate freshness
     rule (or `freshness: null`, with the reason in a comment).
   - Add real-Snowflake grants and objects to the SQL files.
6. **Gold and the dashboard.**
   - A new `exec_*` model has exactly the grain of its chart, carries `brand` and `brand_name`, and
     its numbers add up (README §3).
   - Add its dataset and charts in `build_bundle.py`. `check_dashboard.py` then requires the
     Brand filter to cover it.
   - Update every count that changes: in the README, the brands, tables and foreign keys: in step 5 of [SKILL.md](../SKILL.md), the `exec_*` tables, the freshness lines and
     the reconciliation lines.
7. **A deployable artifact.**
   - Give it the release treatment of `scripts/lakehouse_release.py`: a reproducible build,
     SHA-256, evidence, an audit, a plan, policy checks, deploying only the reviewed plan, then
     verification.
   - Its credentials are short-lived (OIDC), and a personal access token is refused.
8. **CI.** Add the new paths to the workflow's `paths:` triggers, and run actionlint.
9. **README and diagram.** Update the README sections the table names for the touched part, and
   `docs/pipeline.svg` if it draws that part (see below).

## The README and the diagram

[README.md](../../../../README.md) is the only written doc: the table above names its sections for
each part. Its one diagram, [docs/pipeline.svg](../../../../docs/pipeline.svg), is shown at the top
of the README, before "## 1. Source system". It draws every part in the table but the local entry
point: the CRM lane on the left, the clickstream lane on the right, then Superset and the
activation job. README §2 describes
the two lanes, and the SVG's `<desc>` repeats the drawing in words. A change that adds, removes,
renames or moves a step, a table or an arrow updates the drawing, its `<desc>` and README §2
together.

Check an SVG change by rendering it. ImageMagick cannot handle its CSS variables; headless Firefox can.

## Reading the warehouse

The gold fingerprint (`local/gold_fingerprint.py`) covers only the `exec_*` _tables_. It
skips the view `exec_daily_orders_live`, behind the dashboard's Live tiles. For a change to that
view, or in bronze, silver or `RAW.WEB`, query the objects directly with the pipeline's own
connection:

```bash
set -a; source .env; set +a
PYTHONPATH=dags .venv/bin/python - <<'EOF'
from crm_pipeline.bronze_loader import snowflake_session
with snowflake_session() as conn:
    cur = conn.cursor()
    cur.execute("select count(*), min(business_date), max(business_date) from RAW.WEB.DAILY_FUNNEL")
    print(cur.fetchone())
EOF
```

fakesnow must be running (`scripts/local.sh warehouse up`). Reading is safe while no pipeline run
is writing.
