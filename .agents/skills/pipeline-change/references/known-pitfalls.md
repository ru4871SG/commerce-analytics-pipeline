# Known pitfalls

Traps this repo has already hit. Check the change against each one that applies.

This file grows with the work. When a change hits a trap that is not here, the agent that hit it
adds it, as part of the same change (step 12 of the skill).

## Adding a pitfall

- **What qualifies:** something that cost you time, or could break the pipeline without anyone
  noticing, and that the code and `README.md` do not make obvious. Examples: a tool or emulator
  quirk, a step that reports success without doing the work, an ordering or a piece of state the
  scripts rely on.
- **What does not qualify:** a mistake of your own that a test now catches, or a bug fixed in the
  same change that leaves no trap behind.
- **Verify it first.** Reproduce it, or quote the log line or the code that shows it. Do not record
  a guess.
- **Put it in the section it belongs to** (emulator, orchestrators, data and state, lakehouse
  release and Spark, the local script and CI, the dbt docs and the local site, the dev container
  and the published site), or start a new section when none fits. Use a table row in the emulator
  table, and a bullet elsewhere.
- **Write each entry as:**
  - a bold one-line statement of the trap
  - how to recognize it: the symptom, the error or log line, or the command that shows it
  - what to do instead
  - where it lives: the file, function or setting

  Keep it as short as the entries around it.

- **Keep the file true.** When a change fixes a pitfall, remove the entry or reword it to say what
  is left. When a number changes (a timing, a count), update it.

## The emulator (fakesnow) is not Snowflake

| Missing or different                                                               | What the repo does                                                                                                                        |
| ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| No `CREATE STREAM`, `CREATE TASK`, `SYSTEM$STREAM_HAS_DATA`                        | `dags/crm_pipeline/stream_task.py` stands in; the real objects are in `snowflake/crm_streams_and_tasks.sql`, which never runs here        |
| No dynamic tables                                                                  | `fct_daily_orders_live` is a plain table on the `emulator` target                                                                         |
| No `MERGE` inside an explicit transaction                                          | the stream offset moves right after the MERGE, not in the same transaction                                                                |
| `MERGE` needs a current schema                                                     | `bronze_schema.use_schema` before merging                                                                                                 |
| A NULL timestamp comes back as `1970-01-01`                                        | `bronze_schema.timestamp_or_none`                                                                                                         |
| No file transfer (`PUT`), so no `write_pandas`                                     | the connection extra `load_method: insert`                                                                                                |
| A request that outlives `socket_timeout` is sent again, and fakesnow runs it again | `socket_timeout` 900 s and 500-row INSERT batches; a `StagedRowCountError` means a statement ran twice                                    |
| Results capped at 2,048 rows                                                       | `PREFILTER_MAX_VERSIONS` in `bronze_loader.py`; never page through a large result                                                         |
| Inserts are slow (about 3 s per 500 rows)                                          | the first publish of the clickstream history, and its copy back into `RAW.WEB` after a warehouse reset, take about 4 minutes; plan for it |
| dbt unit tests cannot create their temporary tables                                | `build_args` adds `--exclude-resource-type unit_test` on the `emulator` target, so unit tests run only on real Snowflake                  |
| Only one process may open its DuckDB files                                         | stop stray dbt or Superset processes before `scripts/local.sh warehouse reset`                                                            |
| A `sum` of integers is a `DOUBLE`, a `count_if` a `HUGEINT`, `dateadd` on a date a `TIMESTAMP`; Snowflake keeps `NUMBER` and `DATE` | every `exec_*` model casts each column to its contract type in its final `select`, so both build the same types |
| A `sum` of a `number(p, s)` is a `DOUBLE` too, and its last bits depend on the order of the rows (`45409.815259999865` or `45409.81525999989`) | a model that keeps adding sums up casts each back to its decimals first, as the balances of `exec_mrr_monthly` do (`::number(38, 6)`): otherwise a balance that returns to zero is `-1e-13` and fails a `>= 0` test; tests compare amounts within half a cent |
| Views are not in `information_schema.tables`                                       | the gold fingerprint cannot cover `exec_daily_orders_live`, and the dbt docs' catalog has no entry for it: its columns come from the YAML |

Green on the emulator does not prove a feature that depends on Snowflake-only behavior. Name the
gap in the handover.

## Orchestrators

- **dagster-dbt adds `--select fqn:*` to every `dbt.cli(..., context=context)` call.** For commands
  that are not a model build (`source freshness`, `run-operation`), that selection matches nothing.
  dbt prints `Nothing to do` and exits 0. Call those commands without `context`, and use
  `raise_on_error=False` with `.wait()` and `is_successful()` to handle failure yourself.
  `.stream()` raises on failure before your own check can run.
- **The report's critical path.** A task upstream of `dbt_build` (Airflow) or an asset a dbt
  source depends on (Dagster) can stop gold when it fails. Before adding one, read `dbt_build`'s
  trigger rule and upstream tasks, and the dbt assets' dependencies. Decide on purpose whether the
  new step may block the CRM report, make both orchestrators behave the same way, and say so in the
  docs. Check what the real graph does: for Airflow, list each task's `trigger_rule` and
  `downstream_task_ids`. Do not read it from `docs/pipeline.svg`: it draws the data flow, not the
  task graph.
- **Skips:**
  - In Airflow, an empty merge raises `AirflowSkipException`, and the tasks after it run because
    of `TriggerRule.NONE_FAILED`. `publish_web_funnel` uses `NONE_FAILED_MIN_ONE_SUCCESS`, because
    the branch skips one engine.
  - In Dagster, a step with nothing to do still materializes, with `state = SKIPPED` in its
    metadata. Never use `output_required=False`: it skips everything downstream.
- **A side load that fails, fails the run and the command, though gold is built.** That is
  the design (`load_fx_rates`, as the `lakehouse` group): `airflow dags test` and
  `run_report dagster` exit non-zero, so `airflow demo` stops before the dashboard import and
  the activation. Without a network the exchange-rate load fails this way once its three
  tries are spent (`The ECB API did not answer in 3 tries`), from the first day after the
  seed's last rate. To run offline, comment out `AIRFLOW_CONN_ECB_FX_API` in `.env`: the
  load then skips. Do not make the load swallow the error instead.
- **A dbt source needs its table, whether its load ran or not.** A view over a missing table
  fails `dbt build`, and with it all of gold, and a side load is by design not upstream of dbt.
  The dbt step of each orchestrator calls `raw_tables.ensure_side_tables()` first; a new side
  load adds its table to `SIDE_TABLES` there. The clickstream's `RAW.WEB` tables are there too:
  dbt does not wait for the publish step, and a first batch that fails never reaches it
  (`ERROR freshness of web.daily_funnel`, and no gold, until 2026-11-11).
- **The dagster CLI puts the `.env` of the folder it is started in over the environment.**
  `dagster job execute` started in the repo's root loads the repo's `.env` and overrides what
  the calling shell exported or unset (`Loaded environment variables from .env file: ...` in
  its log). So `unset AIRFLOW_CONN_ECB_FX_API` in `ci_scenario` switched the FX load off for
  Airflow and not for Dagster: one skipped, the other ran, with nothing failing but the
  scenario's own evidence line. `run_dagster_job` now starts the job in `dagster/`, which has
  no `.env`; `scripts/local.sh` has sourced `.env` itself. `dagster dev` and `dagster asset
  materialize` (the UI, the simulated days) are still started in the caller's folder: a
  setting changed for them must be changed in `.env`. Never put a `.env` in `dagster/`.
- **One orchestrator at a time.** The offsets, watermarks and ledger are shared. `scripts/local.sh`
  refuses to start one while the other runs. `max_active_runs=1` (Airflow) and the `crm/report`
  tag limit (Dagster) keep one report at a time. Do not loosen either.
- **No scheduler when run from the CLI.** `airflow dags test` and `dagster job execute` run a single
  job: assets and sensors do not start the next one, so `local.sh` runs the activation job itself.
  `dags test` also runs tasks one at a time, so timings differ from the scheduler.
- **A `dags test` run that was killed stays `running` in Airflow's database, and the next
  scheduler finishes it.** `pipeline`, `airflow demo` and `ci-scenario` run the report with
  `airflow dags test`; stopped part-way (a kill, a timeout, Ctrl+C), the run and its unstarted
  tasks stay in `.airflow/airflow.db`. When `fullrun` or `airflow ui` next starts a scheduler,
  it runs those leftover tasks (loads, captures, merges, even `dbt_build`), of every such run
  at once: `max_active_runs=1` only keeps new runs from starting. The new run prints
  `still queued: max_active_runs is 1` and waits, for good when a task of the old run was
  `running` when it died. Before a scheduler starts after any interrupted run, list them
  (`scripts/local.sh airflow dags list-runs crm_executive_report --state running`) and mark
  each failed (the UI, or `PATCH /api/v2/dags/<dag>/dagRuns/<run>` with `{"state": "failed"}`).
- **New DAGs start paused, and Dagster schedules and sensors start stopped.** That is deliberate;
  keep it that way.
- **A bare `python3` in a `BashOperator` or a subprocess gets whatever is first on the PATH.**
  `scripts/local.sh` puts `.venv/bin` first, so a missing package can go unnoticed locally and fail
  on another worker. Name the interpreter of the virtualenv that has the packages, or use
  `sys.executable` when the current one is right.
- **The dbt manifest Dagster loads** (`dbt/target/dagster/manifest.json`) is rebuilt only when a dbt
  file is newer than it. Airflow writes to `dbt/target/airflow/<task_id>`. The two never share a
  target folder.

## Data and state

- **A rebuilt source needs a reset warehouse.** Incremental loads (a newer `updated_at`) cannot see
  a source that was replaced wholesale. `airflow demo` resets fakesnow when it rebuilds the source; by hand,
  run `scripts/local.sh warehouse reset`.
- **The simulator writes days in order, up to yesterday in New York.** Re-running on the same day
  adds nothing, which is what makes a before/after comparison possible on one day.
- **A clickstream file is written once.** A partial day in the real landing volume would make
  tomorrow's full export a no-op. Test generators and engines on a scratch root:

  ```bash
  set -a; source .env; set +a
  export CRM_LAKEHOUSE_ROOT=<scratch>
  PYTHONPATH=dags .venv/bin/python -m crm_pipeline.clickstream --from <day> --to <day>
  ```

  Then plan and run in Python (`crm_pipeline.lakehouse.plan`, then `run_in_process` or
  `run_on_spark`). Never call `publish`: it replaces whole days in `RAW.WEB`, and a scratch
  lakehouse holds partial days (late sessions belong to the day before).

- **Both engines write the same Delta tables.** The tables therefore use the protocol delta-rs and
  Spark both support: no deletion vectors, no liquid clustering, no column mapping
  (`contract.table_properties`). A new column goes into `contract.py` and into both engines, and
  the parity tests must pass alternating engines batch by batch.
- **The deployed Spark job checks the contract fingerprint.** While `lakehouse workspace up` runs,
  `run_on_spark` triggers the _deployed_ wheel, and a batch planned by newer source code fails with
  `ContractMismatch`. Release and deploy again (with a `version` bump in
  `lakehouse/pyproject.toml`), or stop the workspace to test from the source tree. Always state
  which of the two you tested.
- **The `exec_*` columns are a contract with the dashboard, checked in three places.** A rename
  or a retype must be made in all of them, or one of the checks fails:
  - the model's final `select` and `dbt/models/gold/reporting/_reporting.yml`: dbt fails the
    build, before it replaces the table, when they disagree on a column's name or kind of type
    (`This model has an enforced contract that failed`)
  - `local/superset/build_bundle.py` (the dataset, its metrics, the charts) and `_exposures.yml`:
    `tests/test_dashboard_contract.py` fails when one of them reads a column the YAML does not
    declare, or declares another type
  - the dashboard itself: `check_dashboard.py` queries every chart
- **The Superset bundle's hash changes with every build.** `executive_report.zip` stores each
  file's modification time, so `sha256sum` differs between two builds of an unchanged
  `build_bundle.py`. The hash the skill asks you to record identifies the file you imported; it
  does not say whether the dashboard changed. For that, compare `N datasets, M charts` and the
  diff of `build_bundle.py`.
- **dbt's contract check compares kinds of type, not precision or scale.** It reads both sides'
  types from a query's result, where `int` and `number(18, 2)` are both `FIXED`. So `cac_usd`
  declared `int` while its model casts it to `number(18, 2)` passes the build, and the table is
  then created with the declared type: 15.98 became 16, with `PASS` and no warning. Keep the cast
  in the final `select` and the `data_type` in the YAML identical;
  `test_the_final_select_casts_every_column_to_the_type_its_contract_declares` checks it, and the
  gold fingerprint shows a column that was rounded.
- **The gold fingerprint covers a column only when its type is one it knows**: a number, which
  it sums, or a yes/no, of which it counts the yeses. A `count_if` with no cast is a `HUGEINT`
  on the emulator, which `gold_fingerprint.py` does not sum, so a change in it never shows in the
  diff. The enforced contracts end this for the `exec_*` tables: dbt creates each table with the
  declared types. What is left: a column that becomes a number appears in the fingerprint diff as
  new, with no value before, as the five `new_customers_*` columns of `exec_kpi_snapshot` did.
  Check such a column against an independent count. Text and dates are never in the fingerprint:
  a wrong `direction` or `date_day` needs a dbt test or a direct query.
- **A z-score on eight values flags half of all days.** `exec_kpi_anomalies` compares a day with
  the same weekday of the eight weeks before. Eight values make a narrow spread by chance, and a
  count that is usually 0 or 1 has a spread of exactly zero (17% of the table's rows), so on the
  robust z-score alone 53% to 62% of a brand's days had a flag. The minimum difference per metric
  (`anomaly_min_difference` in `dbt_project.yml`) is what makes a flag mean something: with it,
  3% to 8% of days. After a change to those vars, to the metrics, or to how the simulator makes a
  day, run the backtest again (the share of days with a flag per brand, from the table) and
  tune the minimums, not the 3.5. The share also moves with the window: one brand reached 14%
  in its worst 90 days of the last eight months.
- **`exec_web_channel_funnel.cost_per_purchase_usd` can differ by a cent between builds of the same
  data.** It rounds a float sum, so on an exact half-cent tie (384.72 / 16 = 24.045) the sum's last
  bits decide. It shows as a 1-cent difference in that column's fingerprint, between Airflow and
  Dagster or between rebuilds. When it is the only difference, it is not a parity failure: find the
  tie (`ad_spend_usd * 100 / purchases` ending in .5) to be sure.
- **The fingerprint's last digit of `exec_kpi_anomalies.baseline_median` can differ between two
  builds of the same data, Airflow's and Dagster's too.** The column holds medians of eight
  values, so halves, with a 5 in the seventh decimal on some rows. With an odd number of those
  (91 on the data of 2026-11-09) the column's exact sum, `106664.9265925`, lies exactly between
  two values of six decimals, and the order the rows were stored in decides which one the
  float sum rounds to: one table summed in five orders gave `…49988` to `…50017`. That order
  is not the same in every build of one SQL text: on 2026-11-10 an Airflow build gave `…593`
  and the Dagster build after it `…592`. The fixed scenario's sum is on such an edge too
  (`96861.1097015`), so CI's parity step could fail on it. `gold_diff.py --parity` tolerates
  this one digit of this one column (`ROUNDING_TIES`); a plain `diff` of two fingerprints, and
  the comparison with a baseline, still show it. When it is the only difference, prove that the rows are
  equal with a sum the order cannot change, after each build:
  `select sum(round(baseline_median * 10000000)) from ANALYTICS.GOLD.exec_kpi_anomalies` (whole
  numbers add up exactly). Treat anything larger, or the same digit of another column, as a
  real change.
- **A product with no cost SKU costs nothing, and takes its order's fixed costs with it.**
  `int_order_item_costs` leaves such a line's cost NULL (the old pipeline's rule), also when it
  is the order's first line, the one that carries shipping, processing and payment fees. So in
  `exec_product_performance` the three products without one (Daily Focus, Night Cream, Clean
  Protein: `line_cost_usd` is 0) show their whole revenue as margin, and "Margin by product"
  ranks them above products that sell more units. The contribution margin of their orders has
  the same gap. It is the cost rule, not an error of the product table: say so wherever margin
  is shown by product, and do not correct it in a reporting model.
- **A subscription order that was never paid has no renewals and no cancellation.** It stays
  `is_subscription` and not cancelled for good, so counting `is_subscription` reports it as active
  forever. Reporting models use `fct_orders.is_paid_subscription`.
- **What a subscription renews is one unit of its first line, not its first order.** The first
  order's `Subscribe` line has 2 or 3 units on 19% of the subscriptions, and the order can add
  an upsell line, shipping and a 10% line discount; every renewal is one line of one unit at
  the same unit price. Recurring revenue taken from the first order's total, or from a renewal
  that happens to carry the discount, is wrong with no test failing. Use
  `dim_subscriptions.mrr_usd`, and `dim_subscriptions` for a subscription's start, end and
  state, so a model has the one rule for a subscription cancelled with no date.
- **A subscription cancelled with no date is new and churned in the same month.** It ends on
  the day it started (`dim_subscriptions.ended_on = started_on`) and is never in a balance.
  `exec_daily_brand_metrics` cannot place such a cancellation on a day, so it does not count
  it: the daily table has 3 cancellations fewer than `exec_mrr_movements` has churned
  subscriptions (scripted customer 5 of each brand). That difference is right, and
  `assert_mrr_movements_reconcile_with_daily_metrics` holds it; do not make the two equal.
- **The pause flag has no history, and a paused subscription can still be cancelled.**
  `subscription_paused` is the flag as it is now, so `closing_paused_mrr_usd` of a past month is
  the MRR of the subscriptions running then that are paused today. Counting "paused and not
  cancelled" instead would change a closed month when the simulator later cancels one.
- **Logical decoding ignores table privileges, and the table filter is the reader's.** The
  role `crm_cdc_reader` has `REPLICATION` and `LOGIN` and gets `permission denied for table
  orders` on a `select`, yet it decodes every change of that table from the slot. Read without
  wal2json's `add-tables` option, the same slot streams `customers` with its `email` column.
  Nothing on the server restricts a wal2json slot to a table. `crm_pipeline/cdc.py` therefore
  passes `add-tables` on every read, refuses a change of any other table, applies
  `SourceTable.keeps` to every column, and never logs a message. A new capture goes through
  those functions; do not read a slot with a query of your own, and treat the role's password
  as a read-all login's.
- **A replication slot may only be moved to a commit.** `pg_replication_slot_advance` accepts
  any position, and the reading function's `lsn` column has one for every row. Moved to a
  change's own position, in the middle of its transaction, the next read returns that whole
  transaction again, on every run (measured: advanced to the second insert of a transaction,
  the next read started with its `B` row). The capture advances to the `lsn` of the last `C`
  row it read (`cdc.read`), and moving a slot backwards is an error (`cannot advance
  replication slot to X, minimum is Y`).
- **A slot on a quiet database falls behind the server, with nothing to read.** Every database
  writes to one write-ahead log, and a slot only moves when it is read and advanced. With
  17.6 MB written by another database, a slot with no change of its own was 17.6 MB behind
  (`pg_current_wal_flush_lsn() - confirmed_flush_lsn`), and a read returned nothing to advance
  to. The three brands share one server, so each slot would fall behind by what the other two
  write, keep those log files, and be invalidated at `max_slot_wal_keep_size` (`wal_status =
  lost`, `can no longer access replication slot`). When a read returns nothing, `cdc.capture`
  moves the slot to the flush position it read before that read. Keep the order (position
  first, then the read): a read decodes every record that starts before the flush position it
  finds, so only a position read earlier is proven empty.
- **The change data capture starts when a slot is created, and a slot created again leaves a
  gap.** What changed before is not in `RAW.CRM.ORDERS_CDC`: the seed's rows never are.
  `source-db up` creates the slots after the seed is in, drops them before it reloads a changed
  `fake_crm.sql`, and a volume that is removed takes them with it; a warehouse reset alone
  empties the table while the slots stay where they were. The test
  `assert_cdc_holds_every_version_the_loader_copied` therefore expects only the versions that
  changed after a brand's first captured change and were loaded before its latest capture. In
  Airflow the capture runs after the brand's load; from the CLI Dagster runs it before the
  report, so the versions that run loads are checked after the next capture.
- **A scratch slot that is left behind keeps log files until the disk is full, or 1 GB here.**
  A probe that needs changes in the source creates a scratch database with its own slot and
  drops both in a `finally`, as `.local/evidence/20261110_orders-cdc-wal2json/probe.py` does.
  `scripts/local.sh status` lists every `cdc_*` slot: only the three brands' may be there.
- **A copy of the repo still drives the real `crm-source` container.** `source_db_up` names it in
  `scripts/local.sh`, and reloads it when the copy's `fake_crm.sql` differs. To test beside a
  running stack, patch the container, the volume and the ports in the copy, and its `.env`, first.
- **Renaming a brand needs a full reset.** `fake_crm.sql` drops tables, not databases, so the old
  `crm_<brand>` stays in the volume. The landing files and Delta tables keep the old brand too,
  and the next publish would copy it back into `RAW.WEB`. Remove the volume and the lakehouse, not
  only the warehouse: README §4, "Re-run the entire pipeline with fresh install", does both.
- **`fct_daily_orders_live`.** On Snowflake, dbt does not redeploy a dynamic table whose query
  alone changed: build it once with `--full-refresh`.
- **`fct_order_state_history`** is incremental (`delete+insert`, with a post-hook delete). Changing
  its logic needs a full refresh; say so in the handover.

## Lakehouse release and Spark

- **The Databricks CLI echoes GitHub's OIDC request token when it cannot authenticate.** `run()` keeps
  the last 3,000 characters of its error, so the log held the tail of that token. GitHub's masking did
  not catch the fragment. `say()` removes anything token-shaped, so print through it. A secret scan of
  the code will not find this: only the run's log shows it (`gh run view <id> --log-failed`).
- **`release` refuses a dirty `lakehouse/`.** Only `local` and `dev` accept `--allow-dirty`, and they
  record it. Agents never commit, so their releases are always `-t local --allow-dirty`. `deploy`
  still checks the wheel bytes, the code fingerprint and the plan against that release. New code
  needs a new `version`.
- **`deploy` never retries.** If it fails part-way, read what it says and investigate.
- **Spark needs Java 17 or later.** Its first start downloads Delta Lake's jars into `.local/ivy2`,
  and the warm-up's errors go to `.local/spark_warm_up.log`.
- **Measure the gate before you change it.** When you change its ratios, `PEAK_OVERHEAD_BYTES` or
  the budget, run a real batch in process on a scratch lakehouse. Compare its peak memory
  (`resource.getrusage(resource.RUSAGE_SELF).ru_maxrss`) with the estimate the plan logged, and with
  the same measurement taken after the imports and before the batch.

## The local script and CI

- **`.env` is copied from `.env.local.example` once.** A connection added to the example
  later is not in a `.env` that exists, and its step skips for good with a clean log:
  `FX rates: skipped. No ecb_fx_api connection`. `scripts/local.sh install` now appends each
  setting the example has and `.env` lacks, and prints `Added to .env from
  .env.local.example: ...`; it never changes one that is there, and a line commented out in
  `.env` counts as there (that is how to switch a setting off). After adding a setting to
  the example, run `install` before the after-run, and look for the step's own line in the
  log, not for the absence of an error. CI is not affected: its `.env` is new on every run.
- **bash reads `scripts/local.sh` while it runs it.** An edit of the script during one of its
  long commands (`pipeline`, `demo`, `ci-scenario`) moves the text under it: when the command's
  branch of the `case` ends, bash reads on from its old place in the new file. It happened with
  `dagster pipeline`: the jobs had finished, then the command printed the `status` branch's
  lines and ended with `scripts/local.sh: line 1663: syntax error near unexpected token ';;'`,
  exit 2. It runs whatever is there. Do not edit the script while a command of it runs; if you
  did, run that command again before you use its log as evidence.
- **PostgreSQL 18.6 loads an output plugin only when it is on `output_plugin_libraries`.** The
  default is `pgoutput, test_decoding`, so `pg_create_logical_replication_slot('...',
  'wal2json')` fails, for a superuser too: `library "wal2json" may not be used as an output
  plugin`. The package being installed is not enough. The container is started with
  `-c output_plugin_libraries=pgoutput,test_decoding,wal2json` beside `wal_level=logical`
  (`SOURCE_SERVER_SETTINGS` in `scripts/local.sh`, `command:` in `local/compose.yaml`). A
  setting an older server does not know stops it from starting; the base image is pinned by
  digest, so the two move together.
- **The SQL in `local/source_db/` is the seed: a change to it reloads the source databases.**
  `seed_checksum` covers `fake_crm.sql` and `zz_users.sql`, and a reload wipes the simulated
  days and makes `demo` reset the warehouse. A role, a grant or a slot the pipeline gains goes
  into a file outside that folder, applied idempotently on every `source-db up`, as
  `local/source_cdc.sql` is. The Containerfile in that folder is not SQL: the entrypoint
  ignores it, and it is not in the checksum.
- **The source image is built on first use, from an exact apt pin that the repository can drop.**
  `source_db_up` builds `local/source_db/Containerfile` when the machine lacks the image (8 s
  here; it needs Docker Hub and apt.postgresql.org, in CI too). PGDG keeps a few versions of a
  package (three of `postgresql-18-wal2json` on 2026-11-10); once the pinned one is gone the
  build stops with `E: Version '...' for 'postgresql-18-wal2json' was not found` and no
  PostgreSQL starts. Move the pin to a version `apt-cache madison postgresql-18-wal2json` lists
  in the base image, and read real messages again before trusting the decoder
  (`.local/evidence/20261110_orders-cdc-wal2json/probe.py`).
- **The source container is replaced when its image changes, not when its settings do.**
  `source_db_up` compares the container's image with `SOURCE_IMAGE`, whose tag is the hash of
  the Containerfile. A change to `SOURCE_SERVER_SETTINGS` alone leaves the running container
  as it was: remove the container (`podman rm -f crm-source`, the volume stays) and run
  `source-db up`.
- **bash ignores `set -e` inside a function that is the condition of an `if`, or one side of `||`
  or `&&`.** `if ! run_report airflow; then ...` runs the whole function with errexit off, so a
  failed `airflow dags test` followed by `record_last_orchestrator` returns 0, and the step passes
  with a failed run in its log. Setting `set -e` again inside does not help while the caller is
  in such a context. Call a function that relies on `set -e` as a plain statement. To keep its
  status, use the form of `ci_step` in `scripts/local.sh` (`set +e; ( set -e; "$@" ); status=$?;
  set -e`), and call that as a plain statement too.
- **actionlint runs shellcheck on every `run:` script when it finds shellcheck on the PATH.** A
  GitHub runner has shellcheck; this machine may not. So a bare `uvx --from actionlint-py
  actionlint` can pass here and fail in CI on the same file, as it does on
  `lakehouse.yml:120` (`shellcheck reported issue in this script: SC2012`). Use
  `scripts/local.sh lint`: it gives actionlint the pinned shellcheck everywhere, and ignores
  SC2012 for that one line.
- **sqlfluff's dbt templater opens a warehouse connection to compile each model.** With none to
  reach it stops at the first file: `TMP: dbt tried to connect to the database and failed`.
  `lint_sql` in `scripts/local.sh` starts an empty fakesnow on a free port for it, and keeps
  dbt's target and logs in a temporary folder, so the lint never touches `.local/fakesnow` or
  `dbt/target`. A rule added to `dbt/.sqlfluff` must pass on every existing model: try it on the
  whole project first (`sqlfluff lint models tests` in `dbt/`, with the emulator running).
- **The first version has six ruff `F` findings**, in `generate_fake_crm.py`, `build_bundle.py`,
  `check_dashboard.py`, `test_bronze_loader.py` and `test_config_and_export_api.py`. They are
  left as they are, so ruff on the whole repo fails. `scripts/local.sh lint` runs ruff on the
  changed files and on their copies from the base commit, and fails only on a finding the base
  does not have.
- **The demo's gold changes every day; only the scenario's can be compared across days.**
  `ci-scenario` stops at 2026-10-03 and leaves the machine there. A fingerprint taken after it is
  not the demo's: run `scripts/local.sh airflow demo` to simulate up to yesterday again before
  the next baseline.

## The dbt docs and the local site

- **A full-page picture taken from a window shorter than the page shows half-drawn charts.** The
  browser resizes the window to take the picture, and the charts below the window are caught
  while they redraw: bars at one edge only, an empty plot. Superset had queried all of them; only
  the picture is wrong. `local/site/capture_dashboard.py` makes the window as tall as the
  dashboard before it waits for the charts, and takes the picture without another resize. Keep
  that order when you change it.
- **A bar chart over names leaves out every second name when its bars stand up.** With a
  category on the x-axis (`x_axis="product_name"`), Superset drops the labels that do not fit:
  twelve bars, six names. Nothing fails: the dashboard check counts rows and the capture counts
  drawn charts. Pass `horizontal=True` to `time_series()` in `build_bundle.py` for an axis of
  names. Lying down, the last label of the value axis is cut at the frame when it is as long as
  `$25,000.00`: give such a chart a short format (`$.3~s` writes `$25k`). Look at the picture
  before you copy it into `docs/`.
- **A mixed chart's series type is `bar` or `line`, not the name of a chart type.** With
  `"seriesType": "echarts_timeseries_bar"` Superset draws lines and says nothing ("New /
  cancelled / net subscriptions by month" did, until 2026-11-11). The seven values it draws are
  `MIXED_SERIES_TYPES` in `build_bundle.py`, taken from Superset's bundle (they are not in its
  docs), and a test holds every mixed chart to them. Still look at the picture.
- **A time axis always draws its last label, and at about 60 px a month it touches the one
  before.** At the full width of the dashboard the mixed chart's axis read `OctoberNovember`.
  The check and the capture both pass. Keep a chart of months with bars at 8 columns or fewer
  (`LAYOUT` in `build_bundle.py`), where the label before the last is left out, and look at
  the picture.
- **Playwright says the OS is not supported, and works.** On Fedora, `scripts/local.sh site`
  prints `BEWARE: your OS is not officially supported by Playwright; downloading fallback build
  for ubuntu24.04-x64.` That build runs here. If Chromium does not start on another machine for a
  missing library, `playwright install-deps` installs them and needs root: ask the person.
- **The picture is only as true as the wait.** A chart counts as drawn when Superset answered its
  query with HTTP 200, its loading indicator is gone, and its frame shows neither an error nor
  "no results". A change to how Superset marks those (`data-test-chart-id`,
  `data-test="loading-indicator"`, `role="alert"`, `.ant-empty`) makes the capture pass on a
  broken chart or wait until it times out: after a Superset upgrade, run the capture with one
  chart's query made to fail and see that it says so.

## The dev container and the published site

- **Check the dev container on a copy of the tree, never on the checkout.** The container mounts
  the folder it is opened on, and its `postCreateCommand` is `scripts/local.sh install`: on the
  checkout that rebuilds `.venv*` for the container's paths (`.venv/pyvenv.cfg` holds `home =
  /home/<you>/.local/share/uv/python/...`), and a scenario run inside resets `.local`. The CLI
  also mounts the Git root of the folder it is given (`--mount-workspace-git-root`, on by
  default), so a copy placed inside the checkout with no `.git` of its own would mount the
  checkout after all. `.local/evidence/20261111_devcontainer-pages/devcontainer_up.sh` copies
  the tracked and the new files and `.git` into `.local/devcontainer-check/` (on disk: `/tmp`
  is memory on this machine) and passes `--mount-workspace-git-root false`. Remove the
  container, its two `dind-var-lib-*` volumes, the `vsc-*` images and the folder afterwards,
  each by its name.
- **`podman image prune` removes other people's images.** The store is the machine's, not this
  repo's: it holds other projects' builds. A `prune` at the end of the dev container check
  deleted an untagged image that had been there for days and belonged to another project,
  together with the check's own layers; it could not be restored. Before a cleanup, list what
  is there (`podman ps -a`, `podman image ls -a`, `podman volume ls`) and compare with the list
  from before the work; remove what you made with `podman rm`, `rmi` and `volume rm` and its
  name or ID. Leave untagged layers of your own builds rather than prune them, and say so.
- **Under rootless podman the mounted workspace belongs to root inside the container.** The
  container's user, `vscode`, cannot write it (`owner: root:root; writable by vscode: no`), and
  `install` fails at its first file. `PODMAN_USERNS=keep-id` in the environment of the CLI
  (`npx @devcontainers/cli@<pin> up --docker-path podman ...`) gives the folder to `vscode`. Do
  not put `runArgs` for it into `devcontainer.json`: a Codespace runs Docker, which knows no
  `--userns=keep-id`, and a test holds that the file names no engine.
- **The Docker of the dev container runs inside rootless podman, and says one thing that looks
  like a failure.** `mount: /sys/kernel/security: permission denied.` in the container's start
  is the feature's init script; `dockerd` starts all the same (overlayfs, cgroup v2), builds the
  source image and runs PostgreSQL. This machine has no docker daemon: a Codespace's Docker is
  not what ran here.
- **The CLI forwards no port, and publishing one would reach nothing.** Every UI listens on the
  container's localhost (`127.0.0.1:8088`, `:8080`, `:3000`, `:8770`), which is what an editor's
  forwarding reaches and what `-p` on the container does not. Ask each UI from inside the
  container (`devcontainer exec ... curl http://127.0.0.1:<port>/...`). A UI that is moved to
  another port, or a new one, also goes into `forwardPorts`: a test compares the two.
- **The Pages artifact holds the files with the modes they have.** `actions/upload-pages-artifact`
  archives the folder with `tar` and changes no mode, and under this script's `umask 077` the
  site was `-rw-------` in `drwx------`. `site_build` ends with `chmod -R a+rX`. The action also
  leaves out every file and folder whose name starts with a dot (`--exclude=.[^/]*`): the site
  has none, and its place under `.local/` does not count, since the archive is made from inside
  the folder. Replay the action's own `tar` command on `.local/site` to see what would go up
  (`pages-artifact-probe.txt` in that evidence folder).
- **The published site is the fixed scenario's, as of 2026-10-03, and its dbt docs link to its
  own picture.** The workflow builds it after the scenario, from that run's gold and Superset;
  it never shows the demo's latest day. In those docs the exposure's link is `../dashboard.png`
  (`CRM_EXPOSURE_URL`, set by `site_build`); `scripts/local.sh docs` alone links to Superset on
  the machine. A change to the exposure's `url` must keep both.
- **A comment in `pipeline.yml` that says "secrets." fails a test.** `test_ci_scenario.py` holds
  that the workflow uses no secret by looking for the text `secrets.`, the way a workflow reads
  one. Word a comment another way ("it uses no secret").

## Timings on a 24-core machine, for planning

| Command                                                                           | About      |
| --------------------------------------------------------------------------------- | ---------- |
| `scripts/local.sh install` (virtualenvs exist)                                    | 4 min      |
| `scripts/local.sh test` (the Spark suite is 85 s of it)                           | 2 min      |
| First `airflow demo` (history generation, Spark 62 s, publish 4 min)              | 8 min      |
| A later `pipeline` with nothing new (Airflow)                                     | 1 min      |
| `scripts/local.sh dagster demo` with nothing new (including the dashboard import) | 2 to 3 min |
| `lakehouse release -t local`                                                      | 2 min      |
| `scripts/local.sh ci-scenario --reset` (the first load is 9 min of it)            | 14 min     |
| `scripts/local.sh lint` (about 10 s more when a dbt model or test changed)        | 2 s        |
| `scripts/local.sh site` (the first time, 12 s more to download the browser)       | 40 s       |
| `scripts/local.sh dashboard check` (starts Superset, queries the 50 charts twice) | 3 min      |

Fewer cores change little: pinned to 4 cores (`taskset -c 0-3`), the scenario still takes 13 min
and the tests 1.5 min. The Spark batch slows from 58 s to 89 s, but the emulator's inserts, one
at a time, set the pace: the seed into bronze and the first publish of the clickstream history.

Run long commands in the background and check their logs; do not poll in a tight loop.
