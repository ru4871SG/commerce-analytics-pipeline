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
| Side loads: the ECB's exchange rates    | `dags/crm_pipeline/fx_rates.py`, `raw_tables.py`, `dbt/seeds/fx_rates_ecb.csv` | `crm_executive_report.py`: `side_loads.load_fx_rates` | `dagster_crm/side_loads.py`: `load_fx_rates`, job `crm_side_loads` | `tests/test_fx_rates.py`, `tests/test_dag_integrity.py`, `tests/dagster/test_definitions.py` | §1, §2, §7 |
| Side loads: change data capture of orders | `dags/crm_pipeline/cdc.py`, `raw_tables.py`, `local/source_db/Containerfile`, `local/source_cdc.sql`, `scripts/local.sh` (`source_db_up`, `source_slots`) | `crm_executive_report.py`: `side_loads.capture_orders_cdc_<brand>`, after that brand's load | `dagster_crm/side_loads.py`: `capture_orders_cdc`, job `crm_side_loads` | `tests/test_cdc.py`, `tests/test_dag_integrity.py`, `tests/dagster/test_definitions.py`, the dbt test `assert_cdc_holds_every_version_the_loader_copied` | §1, §2, §4, §7 |
| dbt silver and gold                     | `dbt/`                                                                                                                                                                                      | `dags/crm_pipeline/dbt.py` (`dbt_build`)    | `dagster_crm/dbt_assets.py`                  | dbt's own tests (in `dbt build`), `tests/test_dag_integrity.py`                                  | §2, §3, §6 |
| Activation                              | `digest.py` (reads `exec_kpi_snapshot`, `exec_channel_cac_ltv`, `exec_kpi_anomalies`), `notifications.py`, `local/superset/check_dashboard.py`                                              | `crm_executive_activation.py`               | `dagster_crm/activation.py`                  | `tests/test_digest.py`, `tests/dagster/test_definitions.py`                                      | §3, §4     |
| Dashboard                               | `local/superset/build_bundle.py`, `import_dashboard.py`, `superset_config.py`, `safe_url.py`; dbt's side: `dbt/models/gold/reporting/_reporting.yml` (contracts), `_exposures.yml`           | (none)                                      | (none)                                       | `check_dashboard.py`, `tests/test_dashboard_contract.py`                                         | §3, §4     |
| dbt docs and the local site             | `scripts/local.sh` (`docs`, `site`), `local/site/capture_dashboard.py`, `local/site/build_site.py`                                                                                          | (none)                                      | (none)                                       | `tests/test_site.py`, `tests/test_secret_handling.py`                                            | §3, §4     |
| Dev container                           | `.devcontainer/devcontainer.json`, `Dockerfile`, `devcontainer-lock.json`                                                                                                                    | (the fixed scenario runs inside it)         | (the fixed scenario runs inside it)          | `tests/test_devcontainer.py`; the Dev Containers CLI, on a copy of the tree                      | top, §4    |
| Lakehouse deployment                    | `lakehouse/databricks.yml`, `lakehouse/resources/`, `scripts/lakehouse_release.py`, `local/databricks_workspace.py`, `.github/workflows/lakehouse.yml`, `lakehouse/setup/unity_catalog.sql` | (none)                                      | (none)                                       | release checks, `tests/test_local_workspace.py`                                                  | §4, §8     |
| Local entry point                       | `scripts/local.sh`, `requirements.txt`, `.env.local.example`, `.gitignore`                                                                                                                  | (all)                                       | (all)                                        | `scripts/local.sh test`                                                                          | §4         |
| CI: lint, the fixed scenario, gold diff | `.github/workflows/pipeline.yml`, `scripts/local.sh` (`lint`, `ci-scenario`, `ci-summary`, `dashboard check`), `local/gold_diff.py`, `dbt/.sqlfluff`                                        | (the scenario runs it first)                | (the scenario runs it second)                | `tests/test_ci_scenario.py`, `tests/test_gold_diff.py`                                           | §5, §6     |

The `dagster_crm/` modules are under `dagster/dagster_crm/`; the helpers are under
`dags/crm_pipeline/`. Every Dagster job and schedule is assembled in
`dagster_crm/definitions.py` and `dagster_crm/automation.py`. The report job's selection is the
asset groups `bronze` and `dbt`. The clickstream (group `lakehouse`, job `crm_clickstream`) and
the side loads (group and job `crm_side_loads`) are jobs of their own, so neither can stop gold.

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
   - What the machine needs, the dev container needs too. A runtime or a CLI is copied into
     `.devcontainer/Dockerfile` from an image pinned by tag and digest (no package manager, no
     download: a test holds that); a feature is named at its exact version, and
     `npx @devcontainers/cli@<pin> upgrade --workspace-folder . --dry-run` prints the lockfile
     that fixes its hash. A new UI port goes into `forwardPorts` with a label:
     `tests/test_devcontainer.py` compares them with the ports of `scripts/local.sh`.
   - A container image the repo builds has a Containerfile whose base is pinned by tag and
     digest and whose packages by exact version. Its tag is the file's hash (`SOURCE_IMAGE` in
     `scripts/local.sh`), so a changed Containerfile is another image and the container is
     replaced. Keep `local/compose.yaml` in step with what the script starts.
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
   - An optional connection must skip cleanly when it is absent, as `crm_export_api`,
     `databricks_default`, `ecb_fx_api` and `crm_source_cdc` do.
   - A role or an object on the source server is created by `scripts/local.sh` from a file
     outside `local/source_db/` (`local/source_cdc.sql`, applied on every `source-db up`): that
     folder's SQL is the seed, and a change to it reloads the source databases.
   - `scripts/local.sh install` appends to an existing `.env` each setting of
     `.env.local.example` that it lacks, so a new local connection reaches machines that were
     set up before it. Say in the handover that the person must run `install` once.
5. **Warehouse objects.**
   - Whatever writes a table creates it idempotently (`create ... if not exists`). A table
     that dbt reads and that a step outside the report's own chain writes (a side load, the
     clickstream's publish) goes into `SIDE_TABLES` in `dags/crm_pipeline/raw_tables.py`: the dbt
     step creates those too.
   - Each new raw table becomes a dbt `source`, with a description and a deliberate freshness
     rule (or `freshness: null`, with the reason in a comment).
   - Add real-Snowflake grants and objects to the SQL files.
6. **Gold and the dashboard.**
   - A new `exec_*` model has exactly the grain of its chart, carries `brand` and `brand_name`, and
     its numbers add up (README §3).
   - Every model in `dbt/models/gold/reporting/` has an enforced contract (`dbt_project.yml`).
     Declare each column and its `data_type` in `_reporting.yml`, and cast each column to that
     type in the model's final `select`: `varchar`, `date`, `int` for a count, `float` or a
     `number(p, s)` for an amount or a rate.
   - Add its dataset and charts in `build_bundle.py`, with the Superset type that matches each
     contract type, and add the model to the exposure in `_exposures.yml`.
     `tests/test_dashboard_contract.py` fails until the three agree. `check_dashboard.py` then
     requires the Brand filter to cover it.
   - A chart must return rows on every day, or the activation's dashboard check fails: show
     every row and mark the interesting ones (as "Unusual numbers on the as-of day" does), not
     only the interesting ones. Write a chart's own filter as a SQL expression in
     `adhoc_filters`; `check_dashboard.py` applies those, so it queries what the chart shows.
   - A monthly table starts from the macro `brand_months()` (`dbt/macros/reporting_helpers.sql`):
     every brand with an order, crossed with every month up to the as-of date, the same brands
     and months as `exec_daily_brand_metrics`. A month with nothing in it then has a row of
     zeros, and a test can hold the table against the daily one month by month. Name the column
     `month_start`: `check_dashboard.py` queries `date_day`, `week_start` and `month_start` as
     time axes, and a test fails for a chart over a time column it does not know.
   - A model about subscriptions starts from `dim_subscriptions` (one row per paid subscription:
     `started_on`, `ended_on`, `is_active`, `renewals_count`, `mrr_usd`), not from the flags of
     `fct_orders`: it holds the one rule for when a subscription runs and what it is worth a
     month. What moved recurring revenue comes from the macro `subscription_movements()`, which
     `exec_mrr_movements` and `exec_mrr_monthly` share.
   - A table the digest reads: add it to `read_gold` in `digest.py` and to the `deps` of
     Dagster's `executive_digest`; a test compares the two.
   - Update every count that changes: in the README, the brands, tables and foreign keys: in step 5 of [SKILL.md](../SKILL.md), the `exec_*` tables, the freshness lines and
     the reconciliation lines.
7. **A deployable artifact.**
   - Give it the release treatment of `scripts/lakehouse_release.py`: a reproducible build,
     SHA-256, evidence, an audit, a plan, policy checks, deploying only the reviewed plan, then
     verification.
   - Its credentials are short-lived (OIDC), and a personal access token is refused.
8. **CI.**
   - `pipeline.yml` has no path filter: it runs the lint, the tests and the fixed scenario for
     every pull request. A step the report needs to run goes into `ci_scenario` in
     `scripts/local.sh`, and a line that proves it worked into `ci_report_evidence`.
   - A new lakehouse path goes into the `paths:` triggers of `lakehouse.yml`.
   - A new action is pinned by commit SHA, with its version in a comment, and the same pin in
     both workflows (`tests/test_ci_scenario.py` checks it). A new lint tool gets a version
     constant at the top of `scripts/local.sh`.
   - Run `scripts/local.sh lint`: it runs actionlint with the pinned shellcheck, as the runner does.
   - The `pages` job deploys `.local/site` to GitHub Pages on a push to `main`, when the
     repository variable `PUBLISH_SITE` is `true`. The site is built at the end of the
     `pipeline` job (`scripts/local.sh site`), from the scenario's gold and Superset, so a new
     chart or `exec_*` table is on the published page without a change here. The job that
     builds only reads; the job that writes runs no code of the repository. Keep it that way.
9. **README and diagram.** Update the README sections the table names for the touched part, and
   `docs/pipeline.svg` if it draws that part (see below).

## Adding a side load: a step that feeds dbt and never blocks the report

A side load brings dbt something the report can be built without: the exchange rates are the
first. Everything in the checklist above applies; in addition:

1. **The helper** returns what it did, with `state` `SKIPPED` when its optional connection is
   absent. A connection that is there and fails raises: `PermanentError` once the helper's own
   tries are spent, so neither orchestrator retries it on top.
2. **The table** goes into `SIDE_TABLES` in `dags/crm_pipeline/raw_tables.py`. Both dbt steps
   call `ensure_side_tables()` first, so dbt finds an empty table when the load has never run;
   the helper calls `create_side_tables()` before it writes.
3. **Airflow:** a task in the `side_loads` group of `crm_executive_report.py`, with nothing
   upstream and nothing downstream. `SKIPPED` raises `AirflowSkipException`. A test holds that
   `dbt_build` still waits for `reconcile_bronze` only.
4. **Dagster:** an asset in `dagster_crm/side_loads.py`, in the group `crm_side_loads`, whose
   key is the dbt source's (`raw/<schema>/<table>`); `SOURCE_GROUPS` in `dbt_assets.py` sends
   every source that is not `crm` or `web` to that group, and a table of those two that a side
   load writes is named in `SIDE_LOAD_SOURCES` (`crm.orders_cdc`). The job `crm_side_loads` runs
   wherever `crm_clickstream` does: the activity sensor, a schedule, `run_report dagster`. A
   load per brand is one step for all brands there (`cdc.capture_all`), a task per brand in
   Airflow.
5. **dbt:** the source has `freshness: null` with the reason. Every test on a model that
   reads it is `severity: warn`, so the load can never fail `dbt build`. Keep the history a
   model needs in a seed when gold depends on it, so dbt works offline and in CI.
6. **CI:** the fixed scenario needs nothing outside the runner. `ci_scenario` unsets the
   connection of a load that reaches outside it, and `ci_report_evidence` requires each
   report's log to say that the load ran and skipped. A load whose connection is the runner's
   own (the change data capture's PostgreSQL) stays on, and `ci_report_evidence` requires the
   line that shows what it did: changes of every brand after the simulated days, nothing new
   otherwise.
7. **A second capture of a table the loader already copies** never writes the loader's tables
   (`RAW.CRM.<TABLE>`, `<TABLE>_CHANGES`): it gets a table of its own, and a warn-level dbt test
   holds the two against each other. Its models are not joined into existing gold.

## The README and the diagram

[README.md](../../../../README.md) is the only written doc: the table above names its sections for
each part. Its one diagram, [docs/pipeline.svg](../../../../docs/pipeline.svg), is shown at the top
of the README, before "## 1. Source system". It draws every part in the table but the local entry
point: the CRM lane on the left (with the change data capture of orders beside the loader,
along the lane's left edge), the clickstream lane in the middle, the side loads' lane (the
exchange rates) on the right, then Superset and the activation job. README §2 describes
the lanes, and the SVG's `<desc>` repeats the drawing in words. A change that adds, removes,
renames or moves a step, a table or an arrow updates the drawing, its `<desc>` and README §2
together.

Check an SVG change by rendering it. ImageMagick cannot handle its CSS variables; headless Firefox can.

The README's picture of the dashboard, [docs/superset_example.png](../../../../docs/superset_example.png),
is not drawn by hand. `scripts/local.sh site` captures the whole dashboard into
`.local/site/dashboard.png` once every chart has drawn, and fails if one did not. After a change
to a chart, a dataset or the layout, run it, look at the picture, and copy it over
`docs/superset_example.png`. The same command builds the dbt docs, where the exposure
`executive_report` is the last node of the lineage.

The README opens with "See it without installing": the published page (what `scripts/local.sh
site` builds, deployed by the workflow) and the Codespaces badge (the dev container). A change
to what the site holds, to the dev container or to how the demo is started changes that block
and §4's paragraph on the dev container.

## Reading the warehouse

The gold fingerprint (`local/gold_fingerprint.py`) covers only the `exec_*` _tables_: each one's
row count, the sum of every numeric column, and for a yes/no column how many rows say yes. It
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

The change data capture's tables are not in the fingerprint either: `RAW.CRM.ORDERS_CDC`
(`select _BRAND, _OP, count(*) ... group by 1, 2`) and `ANALYTICS.GOLD.FCT_ORDER_CHANGE_EVENTS`.
The source's side is `scripts/local.sh status`, which shows each replication slot's lag and
`wal_status`. A probe that needs changes in the source uses a scratch database with a slot of
its own, never a brand's, and drops both when it ends.
