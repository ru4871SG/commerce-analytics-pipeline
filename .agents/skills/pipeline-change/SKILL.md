---
name: pipeline-change
description: Plan, build, verify and ship a change to this repo's data pipeline without breaking what already runs. Use when adding a tool, engine, source, orchestrator step, dbt model, dashboard chart, deployable bundle, and when fixing a pipeline bug, and before saying such work is done. Covers the before/after regression baseline, Airflow and Dagster parity, the local checks, the Databricks lakehouse release, and the README and pipeline diagram that must change with the code.
---

# Pipeline Change

This repo runs two pipelines on one schedule and joins them in gold:

- **CRM:** PostgreSQL, then the bronze loader into `RAW.CRM.<TABLE>_CHANGES`, then the stream and
  merge stand-in into `RAW.CRM`, then `reconcile_bronze`, then one `dbt build` of
  `ANALYTICS.SILVER` and `ANALYTICS.GOLD`.
- **Clickstream:** landing files, then the gate, then pandas or Spark, then Delta bronze, silver
  and gold, then `RAW.WEB`, which dbt reads.

Beside them run the **side loads**: steps that feed dbt and that the report never waits for. The
first is the ECB's exchange rates, a public REST API, into `RAW.REF.FX_RATES`. The second is the
change data capture of the orders table: PostgreSQL's write-ahead log, decoded by wal2json,
into `RAW.CRM.ORDERS_CDC`, beside the loader and never into its tables.

Superset reads the `exec_*` tables, and the activation job posts the digest. Airflow (the default)
and Dagster run the same helpers in `dags/crm_pipeline/` and `lakehouse/src/crm_lakehouse/`, one at
a time. The state they share lives outside them. Locally, fakesnow stands in for Snowflake, a folder
for Unity Catalog, and `local/databricks_workspace.py` for the Databricks Jobs API.

Make the smallest change that solves the problem. Prove that the existing pipeline gives the same
results before and after it, and that the new part does what it claims. The proof comes from logs
and numbers, not exit codes.

Agents make local changes only:

- **No git writes.** Never commit, create or switch a branch, or push.
- **No GitHub writes.** Never open, merge or comment on a pull request or an issue.
- **A person takes it from there.** They commit, push and open the pull request, from the
  handover you write at the end (step 8).
- **Local deployments only.** Deploy only to the local stand-in (`-t local`); people deploy `dev`
  and `prod`.

Read these before the first change: First of all, check [README.md](../../../README.md), then read
[references/change-map.md](references/change-map.md) (what moves together) and
[references/known-pitfalls.md](references/known-pitfalls.md) (traps already hit here, kept up to
date by every change: see step 12).

## Workflow

1. **Understand the request and the system.** Read the modules you will touch, and follow every
   caller (`grep -rn <name> dags dagster lakehouse dbt local scripts tests`). Place the
   change on the change map: its orchestrator steps, the shared state it touches, its warehouse
   and lakehouse tables, its dashboard datasets, and the README sections and `docs/pipeline.svg`
   that show it. Run `scripts/local.sh status` and note what is running, so you can leave the
   machine as you found it. Run `git status --short`: changes already there are not yours. Leave
   them alone, and keep them out of the handover.

2. **Record the work and define done.** Name the work `{yyyymmdd}_{workname}`: the date the work
   starts, then an underscore, then a few lowercase words joined by hyphens, for example
   `20261029_dagster-freshness-gate`. `<work>` elsewhere in this skill means this name. It names
   the evidence folder, and it is the branch name you suggest to the person in the handover.
   Write `.local/evidence/<work>/plan.md` (`.local/` is git-ignored) with:
   - the problem
   - the scope (in and out)
   - the plan
   - the risks
   - the acceptance criteria

   Save the starting point beside it: `git rev-parse HEAD > .local/evidence/<work>/base-commit.txt`
   and `git status --short > .local/evidence/<work>/git-status-before.txt`. The acceptance
   criteria always include the regression baseline (step 5). They also include the new part's own
   observable proof: a log line, a row count or a chart. The plan becomes the core of the handover.
   If an issue is wanted, the person creates it.

3. **Check the impact across systems.** Answer each of these in the plan, and settle anything that
   must change together before writing code:
   - **Orchestrator parity:** does the step belong in both `dags/crm_executive_report.py` and
     `dagster/dagster_crm/`? Both call one helper with no Airflow import.
   - **Critical path:** anything upstream of `dbt_build` (Airflow), or of a dbt source asset
     (Dagster), can stop gold. Decide on purpose whether the new step may block the CRM report,
     and write that down. A step that must not block it is a side load: follow "Adding a side
     load" in the change map (its own Dagster job, its table created by the dbt step, its
     models' tests at `warn`).
   - **Shared state:**
     - `RAW.CRM.<TABLE>_CHANGES` (the watermarks)
     - `_STREAM_OFFSETS`, `_TASK_HISTORY` and `_SOURCE_FIGURES`
     - the lakehouse batch ledger
     - the side loads' tables (`RAW.REF.FX_RATES`: append-only, each rate stored once;
       `RAW.CRM.ORDERS_CDC`: append-only, each change stored once)
     - the source's replication slots (`cdc_<database>`): only the capture moves them, and one
       that nobody reads keeps the server's log files
     - `.local/last_orchestrator`

     Only one run at a time; never add a second writer.

   - **Contracts:**
     - dbt sources and their freshness (`dbt/models/bronze/_sources.yml`)
     - the `exec_*` columns Superset reads: their enforced dbt contracts
       (`dbt/models/gold/reporting/_reporting.yml`), the datasets and charts
       (`local/superset/build_bundle.py`) and the exposure (`_exposures.yml`), which
       `tests/test_dashboard_contract.py` holds together
     - the brand registry (`dbt/seeds/brands.csv`)
     - the lakehouse contract (`lakehouse/src/crm_lakehouse/contract.py`) and the deployed job's
       contract fingerprint
   - **Environments:** each tool has its own virtualenv, built from its own `requirements.txt`
     section with exact pins.
   - **Real-system files that never run locally:**
     - `snowflake/crm_streams_and_tasks.sql`
     - `lakehouse/setup/unity_catalog.sql`
     - `lakehouse/databricks.yml` and `lakehouse/resources/`
     - `.env.example`
     - `.github/workflows/lakehouse.yml` and `.github/workflows/pipeline.yml` (its commands run
       locally; the workflow itself, and its `pages` job, run only on GitHub)
     - `.devcontainer/` (a Codespace is GitHub's; locally it runs through the Dev Containers CLI,
       on a copy of the tree: see the pitfalls)

4. **Make a small, focused change.**
   - **Where you work:** in the checkout as the person left it, on whatever branch is checked out.
     Do not create, switch, update or delete branches. Every change stays uncommitted in the
     working tree.
   - **Scope:** fix the root cause and keep existing behavior. Do no unrelated cleanup: note
     what you find in the plan instead.
   - **Where code goes:** shared logic belongs in `dags/crm_pipeline/` or `crm_lakehouse`, never in
     a DAG file or a Dagster module.
   - **Style:** match the file around you: a module docstring that says why, plain names, the
     same density of comments.
   - **A new tool, engine or source:** follow the checklist in the change map. That means its own
     requirements section and virtualenv, `install`, `.gitignore`, `test`, the README's command
     lists, connections and docs.
   - **Secrets:** they come only from connections and environment variables (`AIRFLOW_CONN_*`, the
     git-ignored `.env`). Never put them in code, logs, tests, bundles, evidence or the handover.
     Extend `tests/test_secret_handling.py` for any new setting that carries a secret.
   - **README and diagram:** update the README sections and `docs/pipeline.svg` as part of the
     same change (the change map names them). After a change to the dashboard, refresh the
     README's picture of it too: run `scripts/local.sh site`, look at `.local/site/dashboard.png`,
     and copy it over `docs/superset_example.png`.

5. **Run the local checks and the regression baseline.** Capture the baseline _before the first
   edit_, from the checkout as you found it. If you edited before capturing it, do not undo your
   edits with git: say so in the handover, and let the person decide whether a baseline from a
   clean checkout is needed. Run the "after" half on the same New York calendar day as the
   baseline: the simulator adds a day each morning, and a new day changes gold legitimately. If
   the day changed, ask the person for a fresh baseline.

   ```bash
   B=.local/evidence/<work>; mkdir -p "$B"
   # Before: the checkout as you found it, before any edit
   scripts/local.sh test 2>&1 | tail -30 > "$B/test-before.txt"
   scripts/local.sh airflow demo > "$B/demo-before.log" 2>&1
   .venv/bin/python local/gold_fingerprint.py "$B/gold-before.json"
   scripts/local.sh lakehouse status > "$B/lakehouse-before.txt"

   # After: with your changes in the working tree
   scripts/local.sh lint            # what CI's lint job runs, with its pinned tools: bash -n, shellcheck,
                                    # actionlint, ruff (no new finding), sqlfluff on the changed dbt files
   scripts/local.sh test 2>&1 | tail -30 > "$B/test-after.txt"             # .venv, .venv-dagster, .venv-spark
   scripts/local.sh airflow dags list-import-errors                        # "No data found"
   scripts/local.sh dagster definitions validate                           # "All code locations passed validation."
   scripts/local.sh source-db up && scripts/local.sh warehouse up
   scripts/local.sh pipeline > "$B/airflow-after.log" 2>&1
   .venv/bin/python local/gold_fingerprint.py "$B/gold-airflow-after.json"
   scripts/local.sh dagster pipeline > "$B/dagster-after.log" 2>&1
   .venv-dagster/bin/python local/gold_fingerprint.py "$B/gold-dagster-after.json"
   diff "$B/gold-before.json" "$B/gold-airflow-after.json"          # only the differences you intended
   diff "$B/gold-airflow-after.json" "$B/gold-dagster-after.json"   # must be empty: parity
   ```

   The fingerprint covers the `exec_*` tables, not the view `exec_daily_orders_live`. Query that
   view, and any bronze, silver or `RAW.WEB` table you touched, directly (see "Reading the
   warehouse" in the change map).

   CI runs a different run from this baseline: `scripts/local.sh ci-scenario --reset`, the fixed
   scenario (the seed, then 2026-10-01 to 2026-10-03, under Airflow and then Dagster). Run it
   yourself when the change touches `scripts/local.sh`, the first load, the simulator's days or
   the scenario. It removes the local simulated days, the warehouse and the lakehouse: tell the
   person first, and restore with `scripts/local.sh airflow demo` afterwards. The fingerprint
   must then equal the one you had before the reset.

   Then check that each run did its work: read the logs, not the exit codes.

   ```bash
   for log in "$B"/airflow-after.log "$B"/dagster-after.log; do
     echo "== $log"
     grep -c 'PASS freshness of' "$log"                  # one per source with a freshness rule, in both logs
     grep -c 'rows in the source, .* in bronze' "$log"
     grep -oE 'MERGE_[A-Z_]+ (succeeded|skipped)' "$log" | sort | uniq -c
     grep -oE 'Clickstream: [a-z]+[^[]{0,80}' "$log" | head -3
     grep -oE 'FX rates: (skipped|up to date|asked)[^[]{0,120}' "$log" | head -1   # the side load ran
     grep -oE 'CDC orders: ([a-z0-9_]+ (captured|has nothing new)|skipped)[^[]{0,160}' "$log" | sort -u   # one line per brand
     grep -oE 'Done\. PASS=[0-9]+ WARN=[0-9]+ ERROR=[0-9]+' "$log"
     grep -nE 'Nothing to do|Traceback|ERROR=[1-9]|FAILURE' "$log" | head
   done
   ```

   **Tests the change must add:**
   - the helper's behavior in `tests/` (the suite runs in `.venv` and again in `.venv-dagster`,
     without Airflow)
   - the step's place in each graph, in `tests/test_dag_integrity.py` and
     `tests/dagster/test_definitions.py`
   - what happens when it has nothing to do (Airflow skips it; Dagster materializes it with
     `state = SKIPPED`) and when it fails (what stops, and what still runs)
   - for a lakehouse change, both engines in `lakehouse/tests`, including the parity test

   **Security checks:**
   - Your changes carry no secret. This covers both the uncommitted edits and the new files:

     ```bash
     { git diff HEAD; git ls-files --others --exclude-standard -z | xargs -0 -r cat; } \
       | grep -nE 'AKIA[0-9A-Z]{16}|dapi[0-9a-f]{32}|-----BEGIN [A-Z ]*PRIVATE KEY-----|client_secret'
     ```

     Read every match. The pattern itself, in this skill or in `scripts/lakehouse_release.py`, and
     code that only names `client_secret` (a variable or a setting with no value) are not secrets.
     A key, a token or a value is: remove it.

   - Every section of `requirements.txt` you changed passes an audit:

     ```bash
     awk '/^# \[/ { on = ($2 == "[<section>]"); next } on' requirements.txt > "$B/<section>.txt"
     uv tool run pip-audit==2.10.1 -r "$B/<section>.txt" --progress-spinner off
     ```

   - List in the plan every new role, grant, service principal or connection, each with the least
     it needs.
   - For a security fix, first write a test that reproduces the unsafe behavior, then show that it
     passes and that legitimate use still works.

6. **Build and inspect the exact deployment package.**
   - **Lakehouse bundle:** run `scripts/local.sh lakehouse workspace up`, then
     `scripts/local.sh lakehouse release -t local --allow-dirty`. You never commit, so the release
     always sees uncommitted changes; the evidence records that. It writes
     `.local/releases/local-<time>/`, with `evidence.json` (the base commit, the wheel's SHA-256
     and its identical rebuild, the code fingerprint, tests, audit, plan, secret scan),
     `plan.json` and `config.json`.
   - **New code in `crm_lakehouse`:** bump `version` in `lakehouse/pyproject.toml`.
   - **SBOM:** the release has none yet. For a new deployable package, add one:
     `uv tool run pip-audit==2.10.1 -r "$B/<section>.txt" -f cyclonedx-json -o "$B/sbom-<section>.json"`.
     Also check each new dependency's license (on its PyPI page).
   - **The rest** (DAGs, Dagster code, dbt project, Superset bundle) ships as the person's commit.
     Record the base commit. `scripts/local.sh dashboard` prints "N datasets, M charts": record it,
     with the `sha256sum` of `local/superset/executive_report.zip`.
   - **A new deployable artifact:** give it the same treatment as `scripts/lakehouse_release.py`:
     a reproducible build, a hash, evidence, an audit, a plan, policy checks, deploying only the
     reviewed plan, then verification.

7. **Review the plan.**
   - **Lakehouse:** `plan.json` may only create or update `web_events_spark` and its permissions.
     A delete, a recreate or anything unrelated stops the work until you understand it. The policy
     checks must pass: serverless only, the released wheel, one run at a time, production runs as
     a service principal, nobody above `CAN_MANAGE_RUN`.
   - **Data:** the gold fingerprint diff is the data plan. Explain every difference, or stop.
   - **SQL with no plan tool** (`snowflake/`, `lakehouse/setup/`): list every
     `CREATE`/`ALTER`/`DROP`/`GRANT` in the plan. Nothing may drop or replace a table that holds
     data without a stated reason. Call out any full refresh of an incremental or dynamic model.

8. **Hand the work over to a person.** Stop at local changes: the person commits, pushes, opens
   the pull request and merges. Write `.local/evidence/<work>/handover.md` with:
   - **The files:** the files you changed, added and deleted, from `git status --short`, without
     the changes listed in `git-status-before.txt`.
   - **Branch and commit:** a suggested branch name, `<work>`, and a suggested commit message.
   - **The pull request text:** what changed and why, the scope, the validation results with
     their numbers (test counts, dbt `PASS`, the fingerprint diffs, the chart check), the new
     part's own proof, the risks and limits, the README sections and diagram updated, and the
     pitfalls added.
   - **What CI checks:** `.github/workflows/pipeline.yml` runs for every pull request: the lint,
     the tests, and the fixed scenario under both orchestrators, with a parity check and the
     gold diff against `main` in the job summary. `.github/workflows/lakehouse.yml` covers the
     lakehouse paths. On a push to `main`, when the repository variable `PUBLISH_SITE` is
     `true`, it also publishes the site of that run to GitHub Pages. CI runs three simulated
     days, not today's data, and it cannot show what the new part proves on its own, so your
     local evidence still goes here (never secrets): the baseline, the fingerprint diffs, and
     the new part's proof.

   Then tell the user, in a few lines, what you did and where the handover is. If review comments
   come back, make the changes locally, re-run the affected checks, and update the evidence and
   the handover.

9. **Leave review and merge to people.** A person opens the pull request, CI runs, and reviewers
   review and merge. A green build is not authorization, and once the repository variable
   `DATABRICKS_HOST` is set, a merge into `main` also starts the lakehouse workflow's production
   release, and once `PUBLISH_SITE` is `true` it publishes the site. Flag in the handover
   anything that needs a closer review:
   - changes to shared state
   - credentials, grants or service principals
   - the report's critical path
   - deleting data
   - the release or deploy scripts, or a workflow

10. **Deploy through the controlled process, locally only.** Run
    `scripts/local.sh lakehouse deploy -t local`.
    - **What deploy checks:** that the base commit, the wheel bytes, the code and a fresh plan
      still match the evidence. It then applies the reviewed plan file and verifies.
    - **`dev` and `prod`:** people deploy them. A person runs `dev` with `--release <folder>`;
      `prod` deploys only through the workflow, after a reviewer, with GitHub OIDC.
    - **A failed deployment:** stop and investigate. Do not retry, rebuild or re-plan to get past it.
    - **DAG, dbt and Superset changes:** have no deploy pipeline in this repo. Locally, running
      `airflow demo`, `pipeline` and `dashboard` is the deployment. Say in the handover that the
      production rollout happens elsewhere.

11. **Verify the deployed result.**
    - **Lakehouse:** `scripts/local.sh lakehouse verify -t local` must report no drift, the job
      matching its plan, the deployed wheel and the health check.
    - **Dashboard:** start `scripts/local.sh superset` in the background and wait for
      `curl -s http://127.0.0.1:8088/health` to say `OK`. Then run:

      ```bash
      set -a; source .env; set +a
      SUPERSET_USERNAME=admin .venv-superset/bin/python local/superset/check_dashboard.py
      ```

      It must end "All charts returned data", with the Brand filter covering every chart.

    - **Status:** run `scripts/local.sh status` and `scripts/local.sh lakehouse status`. Each
      CDC slot must be `reserved` and behind by kilobytes at most (what the server wrote since the
      last capture), never megabytes; no slot but the brands' may be left.
    - **The new part:** show its proof, as named in the acceptance criteria.

12. **Retain evidence and close the work.**
    - **Keep:** `.local/evidence/<work>/` and the release folder; their summaries go in the
      handover.
    - **Stop what you started:** Superset, then `scripts/local.sh lakehouse workspace down`. Run
      `scripts/local.sh down` if the services were stopped when you began.
    - **Remove:** scratch lakehouses and folders, a scratch database with its replication slot,
      scratch containers, their volumes and images (each by its name, never with a `prune`),
      and a stale `.local/orchestrator.lock`.
    - **Check:** `git status` shows only your changes and the ones that were there before, and
      nothing is committed.
    - **Record new pitfalls:** if the work hit a trap that
      [references/known-pitfalls.md](references/known-pitfalls.md) does not cover, add it there as
      part of the same change, as that file's "Adding a pitfall" section describes. A trap is
      something that cost you time, or could break the pipeline without anyone noticing, and that
      the code and `README.md` do not make obvious: a tool or emulator quirk, a step that reports
      success without doing the work, an ordering the scripts rely on. Also update or remove any
      entry your change made wrong, for example a pitfall your change fixed. List the pitfall
      changes in the handover.
    - **Close:** tell the user what changed, what was tested and deployed locally, the remaining
      limits, and where the handover is.

## Hard rules

- **Never write to git or GitHub.** Leave every change uncommitted in the working tree.
  - **Forbidden git commands:** `git commit`, `branch`, `switch`, `checkout`, `stash`, `merge`,
    `rebase`, `reset`, `restore`, `pull`, `tag` and `push`.
  - **Forbidden `gh` commands:** any that creates or changes something (`gh pr create`,
    `gh pr merge`, `gh issue create` and the like).
  - **Allowed:** read-only commands, such as `git status`, `diff`, `log`, `show`, `blame`,
    `ls-files` and `rev-parse`, and `gh ... view` or `list`.
  - A person commits, pushes, opens the pull request and merges.
- Never accept a step on its exit code alone. Show from its log or its data that it did the work.
- Never change one orchestrator without the other, unless the plan says why a step belongs to one.
- Never run Airflow and Dagster at the same time, or two report runs at once.
- Never publish from a scratch or partial lakehouse into the real warehouse: a publish replaces
  whole days in `RAW.WEB`.
- Never write a clickstream file for today, or a partial day, into the real landing volume. A file
  is written once, so tomorrow's full export would be skipped.
- Never add a package to a virtualenv where it conflicts. Give it its own `requirements.txt`
  section and virtualenv, with exact pins.
- Never put a secret in code, logs, tests, bundles, evidence or the handover.
- Never edit `local/source_db/fake_crm.sql` by hand: regenerate it with
  `local/source_db/generate_fake_crm.py`. After a source rebuild, reset the warehouse
  (`airflow demo` does it).
- Run dbt locally only through `scripts/local.sh dbt ...` (the emulator target). Never run it
  against `prod`.
- Deploy only to `-t local`. Never deploy to `dev` or `prod`.
- Never leave a part of `docs/pipeline.svg`, or a count in the README or this skill, that the
  change made wrong.
- Never leave a replication slot behind, and never read or move a brand's slot by hand: an
  abandoned slot keeps the server's log files until it is invalidated, and a slot moved by hand
  loses changes for good.
- Never edit `scripts/local.sh` while one of its commands is running: bash reads the file as it
  goes, and the command runs on into whatever text is at its old place.
- Never clean up with a `prune` (`podman image prune`, `system prune`, `volume prune`), or by a
  pattern that can match what you did not make: the container store holds other projects'
  images, containers and volumes. Remove what you created, by its name or ID.
- Never fix unrelated findings in passing. Note them in the plan instead.

## You can only claim the work is done, if it means:

- [ ] The plan exists, with acceptance criteria, and each one is met with evidence.
- [ ] `scripts/local.sh test` passes in all three environments, with new tests for the change
      (behavior, graph shape, the nothing-to-do and failure paths).
- [ ] `scripts/local.sh lint` passes: shellcheck and actionlint are clean, ruff shows no new
      finding in the changed files, and sqlfluff passes on the changed dbt files.
- [ ] `airflow dags list-import-errors` finds nothing; `dagster definitions validate` passes.
- [ ] The Airflow and Dagster runs both show freshness, reconciliation, merges, the clickstream
      plan, the side loads and dbt `ERROR=0` in their logs, and their gold fingerprints are
      identical.
- [ ] The gold fingerprint differs from the baseline only where intended, and each difference is
      explained.
- [ ] The dashboard check passes against the new gold.
- [ ] If the lakehouse changed, the release (`--allow-dirty`), deploy and verify all pass for
      `-t local`, and the version is bumped for new code.
- [ ] The README, `docs/pipeline.svg` and the counts match the code (see the change map), and
      `docs/superset_example.png` shows the dashboard as it is now.
- [ ] Every new trap the work hit is in `references/known-pitfalls.md`, and no entry there is made
      wrong by the change.
- [ ] No secrets in the changes, and every changed requirements section is audited.
- [ ] Services, scratch folders and locks are back to how you found them.
- [ ] Every change is uncommitted in the working tree: no commit, branch, push, pull request or
      issue was made.
- [ ] `.local/evidence/<work>/handover.md` lists the changed files, a suggested branch name and
      commit message, and the pull request text, ready for a person to commit and push.
