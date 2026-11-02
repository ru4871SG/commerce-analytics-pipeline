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
  release and Spark), or start a new section when none fits. Use a table row in the emulator
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
- **One orchestrator at a time.** The offsets, watermarks and ledger are shared. `scripts/local.sh`
  refuses to start one while the other runs. `max_active_runs=1` (Airflow) and the `crm/report`
  tag limit (Dagster) keep one report at a time. Do not loosen either.
- **No scheduler when run from the CLI.** `airflow dags test` and `dagster job execute` run a single
  job: assets and sensors do not start the next one, so `local.sh` runs the activation job itself.
  `dags test` also runs tasks one at a time, so timings differ from the scheduler.
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
- **The `exec_*` column names are Superset's contract.** Renaming one breaks charts. Run
  `check_dashboard.py` after any gold change.
- **The gold fingerprint does not see `count_if` columns on the emulator.** They are `HUGEINT`,
  which `gold_fingerprint.py` does not treat as numeric, so a change in them does not show in the
  diff: the column is simply not in `sums`. Cast them (`count_if(...)::int`) when the fingerprint
  must cover them, as `exec_subscription_survival` and the subscription counts in
  `exec_kpi_snapshot` do.
- **`exec_web_channel_funnel.cost_per_purchase_usd` can differ by a cent between builds of the same
  data.** It rounds a float sum, so on an exact half-cent tie (384.72 / 16 = 24.045) the sum's last
  bits decide. It shows as a 1-cent difference in that column's fingerprint, between Airflow and
  Dagster or between rebuilds. When it is the only difference, it is not a parity failure: find the
  tie (`ad_spend_usd * 100 / purchases` ending in .5) to be sure.
- **A subscription order that was never paid has no renewals and no cancellation.** It stays
  `is_subscription` and not cancelled for good, so counting `is_subscription` reports it as active
  forever. Reporting models use `fct_orders.is_paid_subscription`.
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

## Timings on a 24-core machine, for planning

| Command                                                                           | About      |
| --------------------------------------------------------------------------------- | ---------- |
| `scripts/local.sh install` (virtualenvs exist)                                    | 4 min      |
| `scripts/local.sh test` (the Spark suite is 85 s of it)                           | 2 min      |
| First `airflow demo` (history generation, Spark 62 s, publish 4 min)              | 8 min      |
| A later `pipeline` with nothing new (Airflow)                                     | 1 min      |
| `scripts/local.sh dagster demo` with nothing new (including the dashboard import) | 2 to 3 min |
| `lakehouse release -t local`                                                      | 2 min      |

Run long commands in the background and check their logs; do not poll in a tight loop.
