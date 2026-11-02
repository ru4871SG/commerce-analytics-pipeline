#!/usr/bin/env bash
# One local entry point for the whole demo. See README.md

set -euo pipefail
umask 077

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_VERSION="3.12"
AIRFLOW_VERSION="3.3.2"
CONSTRAINTS="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
CONSTRAINTS_SHA256="821effd0f491975682f2774fca3427d7bc2f59ca6de68dcb413ad2c8f890e58c"
FAKESNOW_PORT="${FAKESNOW_PORT:-64616}"
SOURCE_IMAGE="docker.io/library/postgres:18"
SUPERSET_PORT="${SUPERSET_PORT:-8088}"

export AIRFLOW_HOME="$REPO/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$REPO/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="True"
# The UI and API listen on loopback only. Task workers call the
# API server back at localhost address.
export AIRFLOW__API__HOST=127.0.0.1
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://127.0.0.1:8080/execution/
export DBT_BIN="$REPO/.venv-dbt/bin/dbt"
export PATH="$REPO/.venv/bin:$PATH"
# The lakehouse: a folder standing in for Unity Catalog, and the Spark engine
# in its own virtualenv, as dbt is. .env may point CRM_LAKEHOUSE_ROOT elsewhere.
export CRM_LAKEHOUSE_ROOT="${CRM_LAKEHOUSE_ROOT:-$REPO/.local/lakehouse}"
export CRM_SPARK_PYTHON="$REPO/.venv-spark/bin/python"
export CRM_SPARK_IVY_DIR="$REPO/.local/ivy2"
export CRM_SPARK_WAREHOUSE_DIR="$REPO/.local/spark-warehouse"
# When the local workspace runs, the Spark engine is its deployed job, as on Databricks
if [ -f "$REPO/.local/databricks_workspace.pid" ] && kill -0 "$(cat "$REPO/.local/databricks_workspace.pid")" 2>/dev/null; then
    export CRM_LAKEHOUSE_LOCAL_WORKSPACE="http://127.0.0.1:8770"
fi
DATABRICKS_CLI_VERSION="1.21.0"

# Local connections live in .env; if it is absent, use the committed local example.
if [ -f "$REPO/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/.env"
    set +a
elif [ -f "$REPO/.env.local.example" ]; then
    set -a
    # shellcheck disable=SC1091
    source "$REPO/.env.local.example"
    set +a
fi

engine() {
    if command -v podman >/dev/null; then echo podman
    elif command -v docker >/dev/null; then echo docker
    else echo "No podman or docker found." >&2; return 1
    fi
}

requirements_section() {   # the lines of section "# [<name>]" of requirements.txt
    awk -v name="[$1]" '/^# \[/ { on = ($2 == name); next } on' "$REPO/requirements.txt"
}

make_venv() {   # make_venv <dir> <requirements.txt section> [constraints]
    local dir="$1" section="$2" constraint="${3:-}" requirements
    requirements="$(mktemp)"
    requirements_section "$section" > "$requirements"
    if ! grep -q '^[^#[:space:]]' "$requirements"; then
        echo "requirements.txt has no section [$section]." >&2
        rm -f "$requirements"
        return 1
    fi
    if command -v uv >/dev/null; then
        [ -d "$dir" ] || uv venv --python "$PYTHON_VERSION" "$dir"
        uv pip install --python "$dir/bin/python" -r "$requirements" ${constraint:+--constraint "$constraint"}
    else
        [ -d "$dir" ] || "python$PYTHON_VERSION" -m venv "$dir"
        "$dir/bin/pip" install -r "$requirements" ${constraint:+--constraint "$constraint"}
    fi
    rm -f "$requirements"
}

superset_env() {
    export SUPERSET_CONFIG_PATH="$REPO/local/superset/superset_config.py"
    export SUPERSET_HOME="$REPO/.superset"
    export FLASK_APP=superset
    mkdir -p "$SUPERSET_HOME"
    # A random SECRET_KEY per install (superset_config.py reads it). The key is generated before
    # the file is opened, so a failing Python never leaves an empty key behind.
    [ -s "$SUPERSET_HOME/secret_key" ] || (
        umask 077
        key="$("$REPO/.venv-superset/bin/python" -c "import secrets; print(secrets.token_urlsafe(48))")"
        printf '%s\n' "$key" > "$SUPERSET_HOME/secret_key"
    )
}

ensure_local_env() {   # .env from the local example, plus a random password for each login it lacks
    local name value added=""
    [ -f "$REPO/.env" ] || cp "$REPO/.env.local.example" "$REPO/.env"
    chmod 600 "$REPO/.env"
    [ -z "$(tail -c1 "$REPO/.env")" ] || echo >> "$REPO/.env"    # end the last line first
    for name in AIRFLOW_PASSWORD SUPERSET_PASSWORD; do
        if ! grep -q "^$name=" "$REPO/.env"; then
            # Assigned before it is written, so a failing Python stops here under set -e
            value="$("$REPO/.venv/bin/python" -c 'import secrets; print(secrets.token_hex(12))')"
            echo "$name=$value" >> "$REPO/.env"
            added="$added $name"
        fi
    done
    set -a
    # shellcheck disable=SC1091
    source "$REPO/.env"
    set +a
    if [ -n "$added" ]; then
        echo "Generated${added} in .env (git-ignored). Log in as admin with those passwords."
    fi
}

write_airflow_passwords() {   # SimpleAuthManager's password file, from AIRFLOW_PASSWORD in .env
    : "${AIRFLOW_PASSWORD:?is not set: run scripts/local.sh install, which generates it in .env}"
    local file="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
    mkdir -p "$AIRFLOW_HOME"
    # Python writes the JSON, so a hand-edited password with quotes in it still gives a valid file
    "$REPO/.venv/bin/python" -c 'import json, os; print(json.dumps({"admin": os.environ["AIRFLOW_PASSWORD"]}))' \
        > "$file"
    chmod 600 "$file"
}

lakehouse_editable() {   # install the lakehouse package, editable, into the virtualenv $1
    if command -v uv >/dev/null; then
        uv pip install --python "$1/bin/python" --no-deps -e "$REPO/lakehouse"
    else
        "$1/bin/pip" install --no-deps -e "$REPO/lakehouse"
    fi
}

databricks_cli() {   # the pinned Databricks CLI in .local/bin, checked against its SHA-256
    local platform sum archive cli="$REPO/.local/bin/databricks"
    case "$(uname -s)-$(uname -m)" in
        Linux-x86_64) platform=linux_amd64; sum=220e7894fe296d7bc20e131e6dc3c71e063b7d2531d568afd2b3f8d7f6869bb9 ;;
        Linux-aarch64|Linux-arm64) platform=linux_arm64; sum=27900190575596418142e9a130e7b56b3a7bfda1d3dfd1f3f3b6a0ca3ec71846 ;;
        Darwin-x86_64) platform=darwin_amd64; sum=0da076184db754e3040151f54eded5ab04f367c14a2e98ec985a2c08bff2e60a ;;
        Darwin-arm64) platform=darwin_arm64; sum=dcf4f4eaa4d5f0fe4caf808dfb6ac4b3a2ba0740258b8e6a56b1e72fd23ef1f9 ;;
        *) echo "No pinned Databricks CLI for $(uname -sm)." >&2; return 1 ;;
    esac
    if [ -x "$cli" ] && "$cli" --version 2>/dev/null | grep -q "v$DATABRICKS_CLI_VERSION\$"; then
        return 0
    fi
    archive="$(mktemp)"
    curl -fsSL "https://github.com/databricks/cli/releases/download/v$DATABRICKS_CLI_VERSION/databricks_cli_${DATABRICKS_CLI_VERSION}_${platform}.tar.gz" -o "$archive"
    if ! echo "$sum  $archive" | sha256sum -c --quiet; then
        echo "The Databricks CLI download does not match its pinned SHA-256." >&2
        rm -f "$archive"
        return 1
    fi
    mkdir -p "$REPO/.local/bin"
    tar -xzf "$archive" -C "$REPO/.local/bin" databricks
    rm -f "$archive"
}

spark_warm_up() {   # start Spark once: checks Java, and downloads Delta Lake's jars into .local/ivy2
    if ! command -v java >/dev/null && [ -z "${JAVA_HOME:-}" ]; then
        echo "The Spark engine needs Java 17 or later (java on PATH, or JAVA_HOME)." >&2
        return 1
    fi
    echo "Starting Spark once (the first start downloads Delta Lake's jars from Maven Central)..."
    if ! "$CRM_SPARK_PYTHON" -c 'from crm_lakehouse.spark_job import spark_session; s = spark_session("warm-up"); print("Spark", s.version, "ready"); s.stop()' \
            2>"$REPO/.local/spark_warm_up.log" | tail -1; then
        # A Java older than 17 gets here. Say why, not only in the log: what Java and Spark's
        # launcher printed (before Python's stack frames), then the exception
        echo "Spark did not start (the whole log is .local/spark_warm_up.log):" >&2
        awk '/^Traceback/ { stack = 1 } !stack { said[++n] = $0 } { last = $0 }
             END { for (i = (n > 10 ? n - 9 : 1); i <= n; i++) print said[i]; if (stack) print last }' \
            "$REPO/.local/spark_warm_up.log" >&2
        return 1
    fi
}

lakehouse_python() {   # the Airflow environment's Python, with the DAG helpers importable
    PYTHONPATH="$REPO/dags" "$REPO/.venv/bin/python" "$@"
}

snowflake_env() {   # write export lines for the snowflake_default connection to $1
    PYTHONPATH="$REPO/dags" "$REPO/.venv-dbt/bin/python" -m crm_pipeline.dbt_env --shell "$1"
}

wait_for_postgres() {
    local e="$1"
    for _ in $(seq 1 60); do
        # Over TCP: while the init scripts run, the entrypoint's server listens on the socket only
        if "$e" exec crm-source pg_isready -h 127.0.0.1 -U postgres -q >/dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    echo "PostgreSQL did not become ready. See: $e logs crm-source" >&2
    return 1
}

source_psql() {   # psql as the superuser inside the container: SQL on stdin, or psql options such as -c
    "$(engine)" exec -i crm-source psql -U postgres -v ON_ERROR_STOP=1 -q "$@"
}

seed_checksum() {   # the version of the seed SQL: changes whenever fake_crm.sql or zz_users.sql does
    cat "$REPO/local/source_db/fake_crm.sql" "$REPO/local/source_db/zz_users.sql" | sha256sum | cut -d' ' -f1
}

# The checksum of the seed the databases were built from lives in the data volume, so it
# disappears with the data when the volume is removed.
SEED_MARKER=/var/lib/postgresql/.fake_crm.sha256
SOURCE_REBUILT=0

source_db_up() {
    local e fresh=0; e="$(engine)"
    if "$e" container inspect crm-source >/dev/null 2>&1 \
            && [ "$("$e" container inspect -f '{{.Config.Image}}' crm-source)" != "$SOURCE_IMAGE" ]; then
        echo "crm-source runs another image; replacing it with $SOURCE_IMAGE."
        "$e" rm -f crm-source >/dev/null
    fi
    if "$e" container inspect crm-source >/dev/null 2>&1; then
        if [ "$("$e" container inspect -f '{{.State.Running}}' crm-source)" != "true" ]; then
            "$e" start crm-source >/dev/null
        fi
    else
        local mount="$REPO/local/source_db:/docker-entrypoint-initdb.d:ro"
        [ "$e" = "podman" ] && mount="$mount,Z"
        # The entrypoint runs the init scripts only into an empty volume
        "$e" volume inspect crm-source-pgdata >/dev/null 2>&1 || fresh=1
        "$e" run -d --name crm-source -e POSTGRES_PASSWORD=postgres \
            -p 127.0.0.1:5433:5432 -v "$mount" -v crm-source-pgdata:/var/lib/postgresql \
            "$SOURCE_IMAGE" >/dev/null
        if [ "$fresh" = 1 ]; then
            echo "PostgreSQL started; the init scripts are creating the three fake CRM databases."
            SOURCE_REBUILT=1
        fi
    fi
    wait_for_postgres "$e"

    local want have
    want="$(seed_checksum)"
    if [ "$fresh" = 1 ]; then
        have="$want"
    else
        have="$("$e" exec crm-source cat "$SEED_MARKER" 2>/dev/null || true)"
    fi
    if [ "$have" != "$want" ]; then
        # The databases were built from an older fake_crm.sql. It drops and recreates its
        # tables, so it is safe to run again; zz_users.sql then grants on the new tables.
        echo "fake_crm.sql changed since the databases were built; reloading them."
        source_psql < "$REPO/local/source_db/fake_crm.sql" >/dev/null
        source_psql < "$REPO/local/source_db/zz_users.sql" >/dev/null
        SOURCE_REBUILT=1
    fi
    if [ "$SOURCE_REBUILT" = 1 ]; then
        # Bronze copies the changed tables incrementally (rows with a newer updated_at), which
        # cannot see a source replaced wholesale. demo resets the warehouse for that.
        echo "The source databases were rebuilt: reset the warehouse before the next load (demo does)."
    fi
    echo "$want" | "$e" exec -i crm-source sh -c "cat > $SEED_MARKER"
    echo "PostgreSQL ready on 127.0.0.1:5433 (user airflow_reader, password airflow_reader)."
}

source_db_down() {
    local e; e="$(engine)"
    "$e" stop -t 10 crm-source >/dev/null 2>&1 || true
}

source_db_reset() {
    local e; e="$(engine)"
    "$e" rm -f crm-source >/dev/null 2>&1 || true
    "$e" volume rm -f crm-source-pgdata >/dev/null 2>&1 || true
    source_db_up
}

warehouse_up() {
    mkdir -p "$REPO/.local/fakesnow"
    if [ -f "$REPO/.local/fakesnow.pid" ] && kill -0 "$(cat "$REPO/.local/fakesnow.pid")" 2>/dev/null; then
        echo "fakesnow already running on 127.0.0.1:$FAKESNOW_PORT."
        return 0
    fi
    "$REPO/.venv-fakesnow/bin/python" "$REPO/local/fakesnow_server.py" \
        "$REPO/.local/fakesnow" "$FAKESNOW_PORT" >"$REPO/.local/fakesnow.log" 2>&1 &
    echo $! > "$REPO/.local/fakesnow.pid"
    for _ in $(seq 1 60); do
        if grep -q READY "$REPO/.local/fakesnow.log" 2>/dev/null; then
            echo "fakesnow ready on 127.0.0.1:$FAKESNOW_PORT (data in .local/fakesnow)."
            return 0
        fi
        sleep 1
    done
    echo "fakesnow did not start. See .local/fakesnow.log" >&2
    return 1
}

dashboard_import() {
    superset_env
    "$REPO/.venv-superset/bin/python" "$REPO/local/superset/build_bundle.py"
    "$REPO/.venv-superset/bin/python" "$REPO/local/superset/import_dashboard.py"
}

warehouse_reset() {
    warehouse_down
    rm -rf "$REPO/.local/fakesnow"
    warehouse_up
}

# --- orchestrator selection -----------------------------------------------------------------
# Airflow and Dagster share PostgreSQL, fakesnow and the stream offsets. Only one runs at a time.

ORCHESTRATOR_LOCK="$REPO/.local/orchestrator.lock"
LAST_ORCHESTRATOR="$REPO/.local/last_orchestrator"

orchestrator_pattern() {   # what the command lines of an orchestrator's own processes match
    if [ "$1" = airflow ]; then
        echo "$REPO/.venv/bin/airflow"
    else
        # dagster dev runs its webserver, daemon and code servers as `python -m dagster...`
        echo "$REPO/.venv-dagster/bin/(dagster|python[0-9.]* -m dagster)"
    fi
}

orchestrator_pids() {   # the PIDs of running processes of an orchestrator, without this script
    local pattern="$1"
    pgrep -f "$pattern" 2>/dev/null | grep -v -e "^$$\$" -e "^$PPID\$" || true
}

process_tree() {   # the PIDs given and every process they started, down to the last generation
    # One ps for the whole table: a pgrep per process takes seconds on Airflow's ~40
    ps -e -o pid=,ppid= | awk -v roots="$*" '
        BEGIN { n = split(roots, r, " "); for (i = 1; i <= n; i++) tree[r[i]] = 1 }
        { parent[$1] = $2 }
        END {
            do {
                added = 0
                for (pid in parent) if (!(pid in tree) && (parent[pid] in tree)) { tree[pid] = 1; added = 1 }
            } while (added)
            for (pid in tree) print pid
        }'
}

stop_orchestrator() {   # stop_orchestrator airflow|dagster: SIGKILL at once, as pkill -9 does, no shutdown
    local name="$1" label pids all lock_name lock_pid
    label="${name^}"
    # The process tree, not a name pattern: Airflow renames its API server and workers
    # ("airflow worker -- LocalExecutor"), so their command lines lose the virtualenv path.
    # shellcheck disable=SC2046
    pids="$(process_tree $(orchestrator_pids "$(orchestrator_pattern "$name")") | sort -un)"
    if [ -z "$pids" ]; then
        echo "$label is not running."
    else
        # Freeze the tree first, so no process starts another between being listed and killed
        while :; do
            # shellcheck disable=SC2086
            kill -STOP $pids 2>/dev/null || true
            # shellcheck disable=SC2086
            all="$(process_tree $pids | sort -un)"
            [ "$all" != "$pids" ] || break
            pids="$all"
        done
        echo "Killing $label ($(echo "$pids" | wc -l | tr -d ' ') processes)."
        # shellcheck disable=SC2086
        kill -KILL $pids 2>/dev/null || true
        # A killed process still takes a moment to exit and free its port; a zombie holds none
        for _ in $(seq 1 50); do
            ps -o stat= -p "$(paste -sd, <<<"$pids")" 2>/dev/null | grep -qv '^ *Z' || break
            sleep 0.1
        done
        echo "$label stopped."
    fi
    if [ -f "$ORCHESTRATOR_LOCK" ]; then
        read -r lock_name lock_pid < "$ORCHESTRATOR_LOCK" || true
        if [ "${lock_name:-}" = "$name" ]; then
            rm -f "$ORCHESTRATOR_LOCK"
        fi
    fi
}

pipe_list() {   # "a, b and c"
    local value="$1"
    if [ -z "$value" ]; then
        return 0
    fi
    echo "$value" | tr '\n' ' ' | sed 's/ *$//; s/ /, /g; s/, \([^,]*\)$/ and \1/'
}

claim_orchestrator() {   # claim_orchestrator airflow|dagster
    local name="$1" other other_pids lock_name lock_pid
    mkdir -p "$REPO/.local"
    if [ "$name" = airflow ]; then other="dagster"; else other="airflow"; fi
    other_pids="$(orchestrator_pids "$(orchestrator_pattern "$other")")"
    if [ -n "$other_pids" ]; then
        echo "${other^} is already running (PID $(pipe_list "$other_pids"))." >&2
        echo "Stop it first: Ctrl+C in its terminal, or scripts/local.sh stop $other" >&2
        exit 1
    fi

    if [ -f "$ORCHESTRATOR_LOCK" ]; then
        read -r lock_name lock_pid < "$ORCHESTRATOR_LOCK" || true
        if [ -n "${lock_pid:-}" ] && kill -0 "$lock_pid" 2>/dev/null; then
            if [ "$lock_name" = "$name" ]; then
                return 0      # the same orchestrator already holds it; do not hide a running UI
            fi
            echo "${lock_name^} is already running (PID $lock_pid). Stop it first: scripts/local.sh stop $lock_name" >&2
            exit 1
        fi
        rm -f "$ORCHESTRATOR_LOCK"     # stale: its process is gone
    fi
    echo "$name $$" > "$ORCHESTRATOR_LOCK"
}

record_last_orchestrator() {   # record_last_orchestrator airflow|dagster
    mkdir -p "$REPO/.local"
    echo "$1 $(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$LAST_ORCHESTRATOR"
}

dagster_env() {
    export DAGSTER_HOME="$REPO/.dagster"
    export PYTHONPATH="$REPO/dagster:$REPO/dags"
    export DBT_BIN="$REPO/.venv-dbt/bin/dbt"
    # dagster dev's dbt preparer looks for `dbt` on PATH, not at DBT_BIN.
    export PATH="$REPO/.venv-dbt/bin:$PATH"
    mkdir -p "$DAGSTER_HOME"
    # A newer dagster.yaml in the repo (a new concurrency limit, say) replaces the copy
    if [ ! -f "$DAGSTER_HOME/dagster.yaml" ] || [ "$REPO/dagster/dagster.yaml" -nt "$DAGSTER_HOME/dagster.yaml" ]; then
        cp "$REPO/dagster/dagster.yaml" "$DAGSTER_HOME/dagster.yaml"
    fi
}

prepare_dagster_manifest() {
    # The credentials go through a pipe, never a file. Declared apart from the assignment, so a
    # failing snowflake_env stops the script under set -e instead of leaving the variables empty.
    local exports
    exports="$(snowflake_env /dev/stdout)"
    # shellcheck disable=SC1090
    source <(printf '%s\n' "$exports")
    mkdir -p "$REPO/.local"
    # Parse only when a dbt file changed since the manifest was written, as dbt_assets.py checks
    # when the definitions load: a parse takes seconds and most commands change nothing in dbt/.
    local manifest="$REPO/dbt/target/dagster/manifest.json"
    if [ -f "$manifest" ] && [ -z "$(find "$REPO/dbt/dbt_project.yml" "$REPO/dbt/packages.yml" \
            "$REPO/dbt/profiles.yml" "$REPO/dbt/models" "$REPO/dbt/macros" "$REPO/dbt/seeds" "$REPO/dbt/tests" \
            -type f -newer "$manifest" -print -quit)" ]; then
        return 0
    fi
    if ! (cd "$REPO/dbt" && DBT_PROFILES_DIR="$REPO/dbt" "$DBT_BIN" parse --target-path target/dagster) \
            >"$REPO/.local/dbt_parse.log" 2>&1; then
        echo "dbt parse failed. See .local/dbt_parse.log" >&2
        cat "$REPO/.local/dbt_parse.log" >&2
        return 1
    fi
}

run_dagster_job() {   # run_dagster_job <job name> [tags JSON]
    local job="$1" tags="${2:-}"
    dagster_env
    if [ -n "$tags" ]; then
        "$REPO/.venv-dagster/bin/dagster" job execute \
            --working-directory "$REPO/dagster" -m dagster_crm.definitions \
            -j "$job" --tags "$tags"
    else
        "$REPO/.venv-dagster/bin/dagster" job execute \
            --working-directory "$REPO/dagster" -m dagster_crm.definitions \
            -j "$job"
    fi
}

run_report() {   # run_report airflow|dagster
    if [ "$1" = dagster ]; then
        # The clickstream is a job of its own, run first so the report reads the day's funnel. A
        # failed batch does not stop the report; it fails the command once the report is built,
        # as a failed lakehouse task fails Airflow's run after dbt_build.
        local clickstream=0
        run_dagster_job crm_clickstream || clickstream=$?
        run_dagster_job crm_executive_report
        record_last_orchestrator dagster
        if [ "$clickstream" != 0 ]; then
            echo "The crm_clickstream job failed; the report was built from the funnel RAW.WEB already held." >&2
            return "$clickstream"
        fi
    else
        airflow dags test crm_executive_report
        record_last_orchestrator airflow
    fi
}

run_activation() {   # run_activation airflow|dagster
    if [ "$1" = dagster ]; then
        run_dagster_job crm_executive_activation
    else
        airflow dags test crm_executive_activation
    fi
}

run_demo() {   # run_demo airflow|dagster: everything up to a dashboard with yesterday on it, once
    source_db_up
    if [ "$SOURCE_REBUILT" = 1 ]; then
        warehouse_reset
    else
        warehouse_up
    fi
    simulate_days "$1"
    clickstream_history
    run_report "$1"
    dashboard_import
    run_activation "$1"
    echo
    echo "Pipeline and dashboard are ready (built by ${1^}). To see the dashboard, in another terminal:"
    echo "  scripts/local.sh superset     -> http://localhost:$SUPERSET_PORT (admin, password in .env)"
}

airflow_ui() {
    write_airflow_passwords
    exec airflow standalone
}

dagster_ui() {
    dagster_env
    mkdir -p "$REPO/.dagster"
    exec "$REPO/.venv-dagster/bin/dagster" dev \
        --working-directory "$REPO/dagster" -m dagster_crm.definitions \
        --host 127.0.0.1 --port 3000
}

simulate_days() {   # write every missing day of shop activity, oldest first, up to yesterday
    local runner="${1:-airflow}" databases keys first last day logical
    databases="$(tail -n +2 "$REPO/dbt/seeds/brands.csv" | awk -F, '$4 == "true" {print $3}')"
    keys="$(tail -n +2 "$REPO/dbt/seeds/brands.csv" | awk -F, '$4 == "true" {print $1}' | paste -sd'|')"
    # The last day every brand has: the earliest of the brands' latest simulated days
    last="$(for db in $databases; do
        source_psql -d "$db" -tA -c \
            "select coalesce(max(created_at::date), '2026-09-30') from orders where id between 20000 and 69999"
    done | sort | head -1)"
    first="$(date -d "$last + 1 day" +%F)"
    yesterday="$(TZ=America/New_York date -d yesterday +%F)"
    if [[ "$first" > "$yesterday" ]]; then
        echo "Shop activity is in up to $last."
        return 0
    fi
    if [ "$runner" = dagster ]; then
        echo "Writing shop activity for $first to $yesterday (one Dagster partition-range run)."
        dagster_env
        if ! "$REPO/.venv-dagster/bin/dagster" asset materialize \
                --working-directory "$REPO/dagster" -m dagster_crm.definitions \
                --select crm_source_daily_activity --partition-range "$first...$yesterday" \
                >"$REPO/.local/simulate.log" 2>&1; then
            echo "crm_source_daily_activity failed for $first...$yesterday. See .local/simulate.log" >&2
            return 1
        fi
        grep -oE "($keys) [0-9]{4}-[0-9]{2}-[0-9]{2}( is already in the CRM|: ).*" "$REPO/.local/simulate.log" \
            | sed 's/ \[crm_pipeline.*//; s/^/  /'
        return 0
    fi

    echo "Writing shop activity for $first to $yesterday (one crm_daily_activity run per day)."
    day="$first"
    while [[ ! "$day" > "$yesterday" ]]; do
        # The 07:00 New York run on the next morning records the day
        logical="$(TZ=America/New_York date -d "$day + 1 day 07:00" --iso-8601=seconds)"
        airflow dags test crm_daily_activity "$logical" >"$REPO/.local/simulate.log" 2>&1 || {
            echo "crm_daily_activity failed for $day. See .local/simulate.log" >&2
            return 1
        }
        grep -oE "($keys) $day( is already in the CRM|: ).*" "$REPO/.local/simulate.log" \
            | sed 's/ \[crm_pipeline.*//; s/^/  /'
        day="$(date -d "$day + 1 day" +%F)"
    done
}

LOCAL_WORKSPACE_PORT=8770

local_workspace_up() {   # the local stand-in for a Databricks workspace (local/databricks_workspace.py)
    if [ -f "$REPO/.local/databricks_workspace.pid" ] && kill -0 "$(cat "$REPO/.local/databricks_workspace.pid")" 2>/dev/null; then
        echo "Local Databricks workspace already running on http://127.0.0.1:$LOCAL_WORKSPACE_PORT."
        return 0
    fi
    mkdir -p "$REPO/.local"
    "$CRM_SPARK_PYTHON" "$REPO/local/databricks_workspace.py" "$LOCAL_WORKSPACE_PORT" \
        >"$REPO/.local/databricks_workspace.log" 2>&1 &
    echo $! > "$REPO/.local/databricks_workspace.pid"
    for _ in $(seq 1 30); do
        if grep -q READY "$REPO/.local/databricks_workspace.log" 2>/dev/null; then
            echo "Local Databricks workspace on http://127.0.0.1:$LOCAL_WORKSPACE_PORT (state in .local/databricks_workspace)."
            return 0
        fi
        sleep 0.5
    done
    echo "The local workspace did not start. See .local/databricks_workspace.log" >&2
    return 1
}

local_workspace_down() {
    local pid
    if [ -f "$REPO/.local/databricks_workspace.pid" ]; then
        pid="$(cat "$REPO/.local/databricks_workspace.pid")"
        if ps -p "$pid" -o command= 2>/dev/null | grep -qF "local/databricks_workspace.py"; then
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$REPO/.local/databricks_workspace.pid"
    fi
}

lakehouse_release() {   # scripts/lakehouse_release.py validate|release|deploy|verify -t <target>, with the pinned CLI
    databricks_cli
    DATABRICKS_CLI="$REPO/.local/bin/databricks" "$REPO/.venv/bin/python" "$REPO/scripts/lakehouse_release.py" "$@"
}

clickstream_history() {   # every day's clickstream file up to yesterday; existing files are kept
    echo "Clickstream export: writing the days the landing volume lacks (the first time, the history since January 2025)."
    lakehouse_python -m crm_pipeline.clickstream
}

warehouse_down() {
    # Only this checkout's server: a stale PID file may name a reused PID, and another clone of
    # this repo may run its own fakesnow.
    local server="$REPO/local/fakesnow_server.py" pid
    if [ -f "$REPO/.local/fakesnow.pid" ]; then
        pid="$(cat "$REPO/.local/fakesnow.pid")"
        if ps -p "$pid" -o command= 2>/dev/null | grep -qF "$server"; then
            kill "$pid" 2>/dev/null || true
        fi
        rm -f "$REPO/.local/fakesnow.pid"
    fi
    pkill -f "$server" 2>/dev/null || true
}

# --- fullrun: the demo run by the Airflow scheduler, with every UI ----------------------------
# demo runs the DAGs with `airflow dags test`, whose task logs never reach the UI. fullrun starts
# Airflow and Superset, has the scheduler run the report and activation DAGs, and keeps both up
# with the local Databricks workspace until Ctrl+C.

AIRFLOW_URL="http://127.0.0.1:8080"
FULLRUN_AIRFLOW=""      # the PIDs of what fullrun started, and stops when it ends
FULLRUN_SUPERSET=""

answers() {   # answers <url>: the URL responds with a success status
    curl -sf -m 2 -o /dev/null "$1"
}

wait_for() {   # wait_for <name> <url> <seconds> <log file>
    for _ in $(seq 1 "$3"); do
        if answers "$2"; then
            return 0
        fi
        sleep 1
    done
    echo "$1 did not answer on $2 within $3 s. See $4" >&2
    return 1
}

airflow_api() {   # local/airflow_run.py ready|run: trigger a DAG through the scheduler and follow it
    "$REPO/.venv/bin/python" "$REPO/local/airflow_run.py" "$@"
}

airflow_start() {   # airflow standalone (scheduler, API server, UI) in the background
    write_airflow_passwords
    airflow standalone >"$REPO/.local/airflow.log" 2>&1 &
    FULLRUN_AIRFLOW=$!
    echo "Starting Airflow on $AIRFLOW_URL (log in .local/airflow.log)."
    wait_for Airflow "$AIRFLOW_URL/api/v2/monitor/health" 180 .local/airflow.log
}

superset_start() {   # the Superset UI in the background; its environment stays in the subshell
    (
        superset_env
        exec "$REPO/.venv-superset/bin/superset" run -h 127.0.0.1 -p "$SUPERSET_PORT" --with-threads
    ) >"$REPO/.local/superset.log" 2>&1 &
    FULLRUN_SUPERSET=$!
    echo "Starting Superset on http://localhost:$SUPERSET_PORT (log in .local/superset.log)."
}

fullrun_stop() {   # stop the Airflow and Superset fullrun started; the other services keep running
    trap - EXIT INT TERM
    # Disowned first, so bash does not report each job as "Killed"
    # shellcheck disable=SC2086
    disown $FULLRUN_AIRFLOW $FULLRUN_SUPERSET 2>/dev/null || true
    if [ -n "$FULLRUN_SUPERSET" ]; then
        # shellcheck disable=SC2046
        kill $(process_tree "$FULLRUN_SUPERSET") 2>/dev/null || true
        echo "Superset stopped."
    fi
    if [ -n "$FULLRUN_AIRFLOW" ]; then
        stop_orchestrator airflow
    fi
}

open_tabs() {   # each URL in a new tab of the default browser, in order: the last one ends up in front
    local url detach
    if [ "$(uname)" = Darwin ]; then
        for url in "$@"; do
            open "$url"
            sleep 1
        done
    elif [ -n "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ] && command -v xdg-open >/dev/null; then
        # In a session of its own, a browser this starts outlives this script and its terminal
        detach="$(command -v setsid || true)"
        for url in "$@"; do
            # shellcheck disable=SC2086
            ($detach xdg-open "$url" >/dev/null 2>&1 &)
            sleep 2    # one at a time: the first may have to start the browser
        done
    else
        echo "No desktop to open a browser on: open the URLs above yourself."
    fi
}

full_run() {   # full_run [--no-browser]
    local browser=1 workspace_was_up="${CRM_LAKEHOUSE_LOCAL_WORKSPACE:-}" report_run failed=""
    local workspace_url="http://127.0.0.1:$LOCAL_WORKSPACE_PORT/"
    local superset_url="http://localhost:$SUPERSET_PORT/superset/dashboard/executive-report/"
    case "${1:-}" in
        "") ;;
        --no-browser) browser=0 ;;
        *) echo "usage: $0 fullrun [--no-browser]" >&2; exit 2 ;;
    esac
    trap 'fullrun_stop; exit 130' INT TERM
    trap fullrun_stop EXIT

    source_db_up
    if [ "$SOURCE_REBUILT" = 1 ]; then
        warehouse_reset
    else
        warehouse_up
    fi
    # The Spark engine is the job deployed to the local workspace, as on Databricks
    local_workspace_up
    export CRM_LAKEHOUSE_LOCAL_WORKSPACE="http://127.0.0.1:$LOCAL_WORKSPACE_PORT"
    if ! curl -sf -m 5 "$CRM_LAKEHOUSE_LOCAL_WORKSPACE/api/2.2/jobs/list?name=web_events_spark" | grep -q '"job_id"'; then
        echo "The local workspace has no lakehouse job yet: releasing and deploying it (the release runs the lakehouse tests)."
        lakehouse_release release -t local
        lakehouse_release deploy -t local
    fi
    if answers "http://127.0.0.1:$SUPERSET_PORT/health"; then
        echo "Superset already running on http://localhost:$SUPERSET_PORT."
    else
        superset_start
    fi

    # The report and activation DAGs run when their assets are updated, and Airflow queues those
    # runs only for DAGs that are on. Paused while the days are written, each runs once, below.
    airflow dags pause --treat-dag-id-as-regex -y '^crm_executive_(report|activation)$' >/dev/null 2>&1 || true
    simulate_days airflow
    clickstream_history

    if answers "$AIRFLOW_URL/api/v2/monitor/health"; then
        echo "Airflow already running on $AIRFLOW_URL."
        if [ -z "$workspace_was_up" ]; then
            echo "It started before the local workspace, so its Spark batches run in .venv-spark, not as the" >&2
            echo "deployed job. For the job: scripts/local.sh stop airflow, then fullrun again." >&2
        fi
    else
        airflow_start
    fi
    airflow_api ready crm_executive_report crm_executive_activation
    report_run="fullrun__$(date -u +%Y%m%dT%H%M%SZ)"
    if airflow_api run crm_executive_report --run-id "$report_run"; then
        record_last_orchestrator airflow
        # dbt does not wait for the clickstream, and the first publish, the whole history, outlasts
        # the loads. The run has finished now, so gold can take what it published.
        "$REPO/scripts/local.sh" dbt build --select stg_web__daily_funnel+ || failed="storefront rebuild"
        dashboard_import
        # The activation checks the dashboard through Superset
        wait_for Superset "http://127.0.0.1:$SUPERSET_PORT/health" 120 .local/superset.log
        airflow_api run crm_executive_activation || failed=crm_executive_activation
    else
        failed=crm_executive_report
    fi
    wait_for Superset "http://127.0.0.1:$SUPERSET_PORT/health" 120 .local/superset.log

    echo
    echo "Ready:"
    echo "  Superset    $superset_url  (admin, password in .env)"
    echo "  Airflow     $AIRFLOW_URL/dags/crm_executive_report/runs/$report_run  (admin, password in .env)"
    echo "  Databricks  $workspace_url  (the local workspace: its jobs, and each run with its Spark log)"
    if [ -n "$failed" ]; then
        echo "The $failed run failed: see its failed tasks above, and their logs in the Airflow UI." >&2
    fi
    if [ "$browser" = 1 ]; then
        open_tabs "$workspace_url" "$AIRFLOW_URL/dags/crm_executive_report/runs/$report_run" "$superset_url"
    fi
    if [ -z "$FULLRUN_AIRFLOW$FULLRUN_SUPERSET" ]; then
        trap - EXIT INT TERM       # both were running already, and keep running
        [ -z "$failed" ] || exit 1
        return 0
    fi
    echo "Ctrl+C stops the Airflow and Superset started here. PostgreSQL, fakesnow and the workspace keep"
    echo "running: scripts/local.sh down stops them."
    # shellcheck disable=SC2086
    wait $FULLRUN_AIRFLOW $FULLRUN_SUPERSET
}

case "${1:-help}" in
    install)
        # Airflow's constraints file, checked against the hash recorded above before it is used
        constraints="$(mktemp)"
        curl -fsSL "$CONSTRAINTS" -o "$constraints"
        if ! echo "$CONSTRAINTS_SHA256  $constraints" | sha256sum -c --quiet; then
            echo "The constraints file for Airflow $AIRFLOW_VERSION does not match CONSTRAINTS_SHA256." >&2
            rm -f "$constraints"
            exit 1
        fi
        make_venv "$REPO/.venv" airflow "$constraints"
        rm -f "$constraints"
        make_venv "$REPO/.venv-dbt" dbt
        make_venv "$REPO/.venv-dagster" dagster
        make_venv "$REPO/.venv-fakesnow" fakesnow
        make_venv "$REPO/.venv-superset" superset
        make_venv "$REPO/.venv-spark" spark
        for venv in .venv .venv-dagster .venv-spark; do lakehouse_editable "$REPO/$venv"; done
        databricks_cli
        mkdir -p "$REPO/.local"
        ensure_local_env
        (cd "$REPO/dbt" && SNOWFLAKE_ACCOUNT=none SNOWFLAKE_USER=none "$DBT_BIN" deps)
        write_airflow_passwords
        airflow db migrate
        mkdir -p "$REPO/.dagster"
        cp "$REPO/dagster/dagster.yaml" "$REPO/.dagster/dagster.yaml"
        prepare_dagster_manifest
        superset_env
        "$REPO/.venv-superset/bin/superset" db upgrade
        # create-admin leaves an existing admin as it is, so the password is reset to .env's too.
        # Flask-AppBuilder takes it only as an argument: briefly visible in the process list.
        "$REPO/.venv-superset/bin/superset" fab create-admin --username admin --firstname Admin \
            --lastname User --email admin@example.com --password "$SUPERSET_PASSWORD" >/dev/null 2>&1 || true
        "$REPO/.venv-superset/bin/superset" fab reset-password --username admin \
            --password "$SUPERSET_PASSWORD" >/dev/null
        "$REPO/.venv-superset/bin/superset" init
        # Last: only the Spark engine needs Java, so without it everything else is installed
        if ! spark_warm_up; then
            echo "Installed, except the Spark engine: install Java 17 or later, then run install again. Until" >&2
            echo "then the CRM loads and dbt run, but a clickstream batch the gate sends to Spark fails (the" >&2
            echo "history, on the first demo)." >&2
            exit 1
        fi
        echo "Installed. Next: scripts/local.sh airflow (Airflow) or scripts/local.sh dagster (Dagster), or use scripts/local.sh fullrun for the entire pipeline (using Airflow)"
        ;;
    source-db)
        case "${2:-up}" in
            up) source_db_up ;;
            down) source_db_down ;;
            reset) source_db_reset ;;
            shell) "$(engine)" exec -it crm-source psql -U postgres "${3:-crm_broomx}" ;;
            *) echo "usage: $0 source-db up|down|reset|shell [database]" >&2; exit 2 ;;
        esac
        ;;
    warehouse)
        case "${2:-up}" in
            up) warehouse_up ;;
            down) warehouse_down ;;
            reset) warehouse_reset ;;
            *) echo "usage: $0 warehouse up|down|reset" >&2; exit 2 ;;
        esac
        ;;
    airflow)
        claim_orchestrator airflow
        shift
        case "${1:-}" in
            "")
                run_demo airflow
                echo "Starting the Airflow UI on http://localhost:8080 (admin, password in .env). Ctrl+C stops it."
                airflow_ui
                ;;
            demo)
                run_demo airflow
                echo "  scripts/local.sh airflow ui   -> http://localhost:8080 (admin, password in .env)"
                ;;
            ui)
                airflow_ui
                ;;
            *)
                exec airflow "$@"          # e.g. scripts/local.sh airflow dags list
                ;;
        esac
        ;;
    simulate)
        claim_orchestrator airflow
        mkdir -p "$REPO/.local"
        simulate_days airflow
        ;;
    pipeline)
        claim_orchestrator airflow
        # dags test runs no scheduler, so the gold asset does not start the activation DAG by itself
        run_report airflow
        run_activation airflow
        ;;
    dagster)
        claim_orchestrator dagster
        dagster_env
        prepare_dagster_manifest
        shift
        case "${1:-}" in
            "")
                run_demo dagster
                echo "Starting the Dagster UI and daemon on http://localhost:3000. Ctrl+C stops them."
                dagster_ui
                ;;
            demo)
                run_demo dagster
                echo "  scripts/local.sh dagster ui   -> http://localhost:3000"
                ;;
            ui)
                dagster_ui
                ;;
            simulate)
                mkdir -p "$REPO/.local"
                simulate_days dagster
                ;;
            pipeline)
                run_report dagster
                run_activation dagster
                ;;
            *)
                dagster_env
                args=("$@")
                if [[ " ${args[*]} " != *" -m "* && " ${args[*]} " != *" --module-name "* \
                        && " ${args[*]} " != *" -f "* && " ${args[*]} " != *" --python-file "* \
                        && " ${args[*]} " != *" --working-directory "* ]]; then
                    case "${1:-}" in
                        asset|job|definitions|dev|schedule|sensor)
                            args+=(--working-directory "$REPO/dagster" -m dagster_crm.definitions)
                            ;;
                    esac
                fi
                exec "$REPO/.venv-dagster/bin/dagster" "${args[@]}"
                ;;
        esac
        ;;
    stop)
        case "${2:-}" in
            airflow|dagster) stop_orchestrator "$2" ;;
            *) echo "usage: $0 stop airflow|dagster" >&2; exit 2 ;;
        esac
        ;;
    superset)
        superset_env
        exec "$REPO/.venv-superset/bin/superset" run -h 127.0.0.1 -p "$SUPERSET_PORT" --with-threads
        ;;
    dashboard)
        dashboard_import
        echo "Dashboard imported: http://localhost:$SUPERSET_PORT/superset/dashboard/executive-report/"
        ;;
    fullrun)
        claim_orchestrator airflow
        full_run "${2:-}"
        ;;
    test)
        shift
        # lakehouse/tests runs in all three: its Spark tests only in .venv-spark, its in-process ones
        # with pandas 3 (.venv) and pandas 2 (.venv-dagster) too
        "$REPO/.venv/bin/python" -m pytest "$REPO/tests" "$REPO/lakehouse/tests" --ignore="$REPO/tests/dagster" "$@"
        dagster_env
        prepare_dagster_manifest >/dev/null
        echo "Running the suite again without Airflow (Dagster environment)..."
        PYTHONPATH="$REPO/dagster:$REPO/dags" \
            "$REPO/.venv-dagster/bin/python" -P -m pytest "$REPO/tests" "$REPO/lakehouse/tests" "$@"
        echo "Running the lakehouse suite with Spark (.venv-spark): both engines, the same tables..."
        "$CRM_SPARK_PYTHON" -m pytest "$REPO/lakehouse/tests" "$@"
        ;;
    dbt)
        shift
        # Through a pipe, as in prepare_dagster_manifest: a failing snowflake_env stops here
        exports="$(snowflake_env /dev/stdout)"
        # shellcheck disable=SC1090
        source <(printf '%s\n' "$exports")
        if [ "${1:-}" = "build" ] && [ "$DBT_TARGET" = "emulator" ]; then
            case " $* " in
                *" --exclude-resource-type "*) ;;
                *) set -- "$@" --exclude-resource-type unit_test ;;
            esac
        fi
        cd "$REPO/dbt"
        export DBT_PROFILES_DIR="$REPO/dbt"
        "$DBT_BIN" "$@"
        ;;
    lakehouse)
        shift
        case "${1:-status}" in
            generate)
                shift
                lakehouse_python -m crm_pipeline.clickstream "$@"
                ;;
            run|status)
                if [ "$1" = run ]; then claim_orchestrator airflow; fi
                lakehouse_python -m crm_pipeline.lakehouse "$@"
                ;;
            test)
                shift
                "$CRM_SPARK_PYTHON" -m pytest "$REPO/lakehouse/tests" "$@"
                ;;
            validate|release|deploy|verify)
                lakehouse_release "$@"
                ;;
            workspace)
                case "${2:-up}" in
                    up) local_workspace_up ;;
                    down) local_workspace_down ;;
                    *) echo "usage: $0 lakehouse workspace up|down" >&2; exit 2 ;;
                esac
                ;;
            reset)
                # The local lakehouse's tables and ledger; the landing files stay, to be loaded again
                rm -rf "${CRM_LAKEHOUSE_ROOT:?}/${CRM_LAKEHOUSE_CATALOG:-local}"
                echo "Removed the local lakehouse tables. The landing files are kept: the next run loads them all."
                ;;
            *) echo "usage: $0 lakehouse generate|run [auto|pandas|spark]|status|test|workspace up|down|validate|release|deploy|verify|reset" >&2; exit 2 ;;
        esac
        ;;
    status)
        e="$(engine)"
        echo "PostgreSQL: $("$e" container inspect -f '{{.State.Status}}' crm-source 2>/dev/null || echo not created) (127.0.0.1:5433)"
        if [ -f "$REPO/.local/fakesnow.pid" ] && kill -0 "$(cat "$REPO/.local/fakesnow.pid")" 2>/dev/null; then
            echo "fakesnow:   running (127.0.0.1:$FAKESNOW_PORT, data .local/fakesnow)"
        else
            echo "fakesnow:   stopped"
        fi
        # Where the Spark engine runs: the job deployed to the local workspace, or .venv-spark
        if [ -n "${CRM_LAKEHOUSE_LOCAL_WORKSPACE:-}" ]; then
            echo "Databricks: local workspace running ($CRM_LAKEHOUSE_LOCAL_WORKSPACE); Spark batches run its deployed job"
        else
            echo "Databricks: local workspace stopped; Spark batches run from the source tree in .venv-spark"
        fi
        if [ -f "$ORCHESTRATOR_LOCK" ]; then
            read -r lock_name lock_pid < "$ORCHESTRATOR_LOCK" || true
            if [ -n "${lock_pid:-}" ] && kill -0 "$lock_pid" 2>/dev/null; then
                echo "Orchestrator: ${lock_name} (PID $lock_pid)"
            else
                echo "Orchestrator: none (stale ${lock_name:-lock})"
            fi
        else
            echo "Orchestrator: none"
        fi
        if [ -d "$CRM_LAKEHOUSE_ROOT" ]; then
            echo "Lakehouse:  $(lakehouse_python -m crm_pipeline.lakehouse status --short 2>/dev/null || echo "not readable")"
        fi
        if [ -f "$LAST_ORCHESTRATOR" ]; then
            echo "Last gold:  $(cat "$LAST_ORCHESTRATOR")"
        else
            echo "Last gold:  not recorded yet"
        fi
        echo "Airflow UI:  http://localhost:8080 (run scripts/local.sh airflow ui)"
        echo "Dagster UI:  http://localhost:3000 (run scripts/local.sh dagster ui)"
        echo "Superset UI: http://localhost:$SUPERSET_PORT (run scripts/local.sh superset)"
        ;;
    down)
        warehouse_down
        source_db_down
        local_workspace_down
        echo "Stopped PostgreSQL and fakesnow. Airflow, Dagster and Superset run in the foreground; stop them"
        echo "with Ctrl+C, or an orchestrator from another terminal with scripts/local.sh stop airflow|dagster."
        ;;
    *)
        case "${1:-}" in
            generate|run|workspace|validate|release|deploy|verify)
                echo "'$1' is a lakehouse command: scripts/local.sh lakehouse $*" >&2
                echo "(release, deploy, verify and validate also need a target, e.g. -t local)" >&2
                exit 2
                ;;
            demo)
                echo "'demo' is part of scripts/local.sh airflow now: that runs it, then starts the UI." >&2
                echo "For the run alone: scripts/local.sh airflow demo" >&2
                exit 2
                ;;
        esac
        sed -n '2,/^set -euo pipefail/p' "$0" | sed '$d; s/^# \{0,1\}//'
        ;;
esac
