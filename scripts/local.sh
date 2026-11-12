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
# The source server: PostgreSQL 18 with wal2json, built from local/source_db/Containerfile when
# this machine does not have it. The tag is that file's hash, so a changed Containerfile is
# another image, and source_db_up replaces the container (the volume, with the data, is kept).
SOURCE_CONTAINERFILE="$REPO/local/source_db/Containerfile"
SOURCE_IMAGE="localhost/crm-source:pg18-wal2json-$(sha256sum "$SOURCE_CONTAINERFILE" | cut -c1-12)"
# What the server is started with, for the change data capture of the orders table
# (dags/crm_pipeline/cdc.py). logical: the write-ahead log carries what logical decoding needs.
# A slot nobody reads keeps its log files: past 1 GB it is invalidated instead of filling the
# disk. PostgreSQL 18.6 loads an output plugin only when it is on its list, and wal2json is not
# on the default one (pgoutput, test_decoding). Keep local/compose.yaml in step.
SOURCE_SERVER_SETTINGS=(-c wal_level=logical -c max_slot_wal_keep_size=1GB
                        -c "output_plugin_libraries=pgoutput,test_decoding,wal2json")
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
# The lint tools of `scripts/local.sh lint`, run through uv without a virtualenv of their own
SHELLCHECK_VERSION="0.11.0.1"
ACTIONLINT_VERSION="1.7.12.25"
RUFF_VERSION="0.17.4"
SQLFLUFF_VERSION="4.5.0"
# The browser automation `scripts/local.sh site` photographs the dashboard with. The version also
# fixes the Chromium build Playwright downloads (into .local/playwright), so one pin covers both.
PLAYWRIGHT_VERSION="1.65.0"

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

# CRM_SIMULATE_UNTIL=YYYY-MM-DD: simulate_days and clickstream_history stop at that day instead of
# yesterday, so a run gives the same data on any day (ci-scenario sets it). Unset: yesterday.
if [ -n "${CRM_SIMULATE_UNTIL:-}" ] && [[ ! "$CRM_SIMULATE_UNTIL" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "CRM_SIMULATE_UNTIL must be a day as YYYY-MM-DD, not '$CRM_SIMULATE_UNTIL'." >&2
    exit 2
fi

engine() {   # podman, else docker; CRM_CONTAINER_ENGINE names one where both are installed (CI)
    case "${CRM_CONTAINER_ENGINE:-}" in
        podman|docker)
            if command -v "$CRM_CONTAINER_ENGINE" >/dev/null; then echo "$CRM_CONTAINER_ENGINE"; return 0; fi
            echo "CRM_CONTAINER_ENGINE=$CRM_CONTAINER_ENGINE, but $CRM_CONTAINER_ENGINE is not installed." >&2
            return 1
            ;;
        "") ;;
        *) echo "CRM_CONTAINER_ENGINE must be podman or docker, not '$CRM_CONTAINER_ENGINE'." >&2; return 1 ;;
    esac
    if command -v podman >/dev/null; then echo podman
    elif command -v docker >/dev/null; then echo docker
    else echo "No podman or docker found." >&2; return 1
    fi
}

usage() {   # the comment at the top of this file
    sed -n '2,/^set -euo pipefail/p' "$REPO/scripts/local.sh" | sed '$d; s/^# \{0,1\}//'
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

ensure_local_env() {   # .env from the local example, plus what it lacks: the example's newer settings, a password per login
    local name value line added="" settings=""
    [ -f "$REPO/.env" ] || cp "$REPO/.env.local.example" "$REPO/.env"
    chmod 600 "$REPO/.env"
    [ -z "$(tail -c1 "$REPO/.env")" ] || echo >> "$REPO/.env"    # end the last line first
    # .env is copied from the example once. A setting the example gained since (a new connection)
    # would never reach it, and the step that needs it would skip for good without anyone
    # noticing. So each of the example's settings .env lacks is appended. One .env has is never
    # changed, and one that is commented out there counts as there: that is how to switch it off.
    while IFS= read -r line; do
        name="${line%%=*}"
        if ! grep -qE "^[# ]*$name=" "$REPO/.env"; then
            printf '%s\n' "$line" >> "$REPO/.env"
            settings="$settings $name"
        fi
    done < <(grep -E '^[A-Za-z_][A-Za-z0-9_]*=' "$REPO/.env.local.example")
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
    if [ -n "$settings" ]; then
        echo "Added to .env from .env.local.example:${settings}. What .env already had is unchanged."
    fi
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

extracted_databases() {   # the source database of each brand the pipeline extracts, one per line
    tail -n +2 "$REPO/dbt/seeds/brands.csv" | awk -F, '$4 == "true" {print $3}'
}

source_image() {   # source_image <engine>: build the source image when this machine does not have it
    local e="$1" context status=0
    if "$e" image inspect "$SOURCE_IMAGE" >/dev/null 2>&1; then
        return 0
    fi
    echo "Building $SOURCE_IMAGE: PostgreSQL 18 with wal2json (local/source_db/Containerfile)."
    # An empty build context: the Containerfile copies nothing, and local/source_db holds the 5 MB seed
    context="$(mktemp -d)"
    "$e" build -t "$SOURCE_IMAGE" -f "$SOURCE_CONTAINERFILE" "$context" >"$context.log" 2>&1 || status=$?
    rmdir "$context"
    if [ "$status" != 0 ]; then
        echo "The source image did not build (it needs Docker Hub and apt.postgresql.org):" >&2
        tail -n 20 "$context.log" >&2
        rm -f "$context.log"
        return "$status"
    fi
    rm -f "$context.log"
}

source_cdc_setup() {   # the CDC role, and a replication slot in each extracted brand's database, when missing
    local db
    for db in $(extracted_databases); do
        source_psql -d "$db" < "$REPO/local/source_cdc.sql" >/dev/null
    done
}

source_cdc_drop_slots() {   # every CDC slot of the server; source_cdc_setup creates them again
    source_psql -d postgres -c \
        "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name like 'cdc\_%'" >/dev/null
}

source_slots() {   # each CDC slot: how far behind the server's log it is, and whether it still has the log it needs
    source_psql -d postgres -tA -c "
        select coalesce(string_agg(
                   slot_name || ' ' || coalesce(pg_size_pretty(pg_current_wal_lsn() - confirmed_flush_lsn), 'no position')
                   || ' behind (' || coalesce(wal_status, 'unknown') || coalesce(', ' || invalidation_reason, '') || ')',
                   '; ' order by slot_name), 'none')
        from pg_replication_slots where slot_name like 'cdc\_%'"
}

source_db_up() {
    local e fresh=0; e="$(engine)"
    source_image "$e"
    if "$e" container inspect crm-source >/dev/null 2>&1 \
            && [ "$("$e" container inspect -f '{{.Config.Image}}' crm-source)" != "$SOURCE_IMAGE" ]; then
        echo "crm-source runs another image; replacing it with $SOURCE_IMAGE (its volume, with the data, is kept)."
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
            "$SOURCE_IMAGE" "${SOURCE_SERVER_SETTINGS[@]}" >/dev/null
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
        # The CDC slots go first, and are created again below: the reload is not a change to capture
        source_cdc_drop_slots
        source_psql < "$REPO/local/source_db/fake_crm.sql" >/dev/null
        source_psql < "$REPO/local/source_db/zz_users.sql" >/dev/null
        SOURCE_REBUILT=1
    fi
    # The CDC role and each brand database's replication slot (local/source_cdc.sql), when they
    # are missing. After the seed is in: the capture starts with the days written from now on.
    # Not in zz_users.sql, whose every change reloads the databases (seed_checksum).
    source_cdc_setup
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
    # Started in dagster/, not in the caller's folder. The dagster CLI reads a .env file in the
    # folder it is started in and puts its values over the environment: started in the repo's
    # root, a setting this script changed or unset would come back from .env (ci-scenario unsets
    # the ECB connection). This script has sourced .env already, so the job's environment is its.
    (
        cd "$REPO/dagster"
        if [ -n "$tags" ]; then
            "$REPO/.venv-dagster/bin/dagster" job execute \
                --working-directory "$REPO/dagster" -m dagster_crm.definitions \
                -j "$job" --tags "$tags"
        else
            "$REPO/.venv-dagster/bin/dagster" job execute \
                --working-directory "$REPO/dagster" -m dagster_crm.definitions \
                -j "$job"
        fi
    )
}

emulator_target() {   # whether snowflake_default is the local emulator, not a real warehouse
    [ "$(PYTHONPATH="$REPO/dags" "$REPO/.venv-dbt/bin/python" -m crm_pipeline.dbt_env | sed -n 's/^DBT_TARGET=//p')" = emulator ]
}

local_services_up() {   # PostgreSQL and fakesnow, when they are not running; a rebuilt source means a new warehouse
    source_db_up
    if [ "$SOURCE_REBUILT" = 1 ]; then
        warehouse_reset
    else
        warehouse_up
    fi
}

pipeline_services() {   # what `pipeline` needs running before its first task
    # Without them no task fails for good: each waits for its retries, and the run stands still.
    # Only on the emulators: against a real warehouse and CRM this machine has nothing to start.
    if emulator_target; then
        local_services_up
    fi
}

run_report() {   # run_report airflow|dagster
    if [ "$1" = dagster ]; then
        # The side loads and the clickstream are jobs of their own, run first so the report reads
        # the day's rates and funnel. A failed one does not stop the report; it fails the command
        # once the report is built, as a failed side-load or lakehouse task fails Airflow's run
        # after dbt_build.
        local clickstream=0 side_loads=0
        run_dagster_job crm_side_loads || side_loads=$?
        run_dagster_job crm_clickstream || clickstream=$?
        run_dagster_job crm_executive_report
        record_last_orchestrator dagster
        if [ "$side_loads" != 0 ]; then
            echo "The crm_side_loads job failed; the report was built from what its tables already held." >&2
            return "$side_loads"
        fi
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
    local_services_up
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

SEED_END="2026-09-30"   # the seed's last day (END in local/source_db/generate_fake_crm.py)

simulated_days() {   # each extracted brand's latest simulated day, one per line (the seed's last day when it has none)
    local db
    for db in $(tail -n +2 "$REPO/dbt/seeds/brands.csv" | awk -F, '$4 == "true" {print $3}'); do
        source_psql -d "$db" -tA -c \
            "select coalesce(max(created_at::date), '$SEED_END') from orders where id between 20000 and 69999"
    done
}

simulate_through() {   # the last day to simulate: CRM_SIMULATE_UNTIL when it is set, otherwise yesterday in New York
    local yesterday
    yesterday="$(TZ=America/New_York date -d yesterday +%F)"
    if [ -z "${CRM_SIMULATE_UNTIL:-}" ]; then
        echo "$yesterday"
    elif [[ "$CRM_SIMULATE_UNTIL" > "$yesterday" ]]; then
        # A day is written once, so one that is not over yet would stay partial for good
        echo "CRM_SIMULATE_UNTIL=$CRM_SIMULATE_UNTIL is after yesterday ($yesterday): only days that are over can be written." >&2
        return 1
    else
        echo "$CRM_SIMULATE_UNTIL"
    fi
}

simulate_days() {   # write every missing day of shop activity, oldest first, up to yesterday (or CRM_SIMULATE_UNTIL)
    local runner="${1:-airflow}" keys first last through before logical
    keys="$(tail -n +2 "$REPO/dbt/seeds/brands.csv" | awk -F, '$4 == "true" {print $1}' | paste -sd'|')"
    # The last day every brand has: the earliest of the brands' latest simulated days
    last="$(simulated_days | sort | head -1)"
    first="$(date -d "$last + 1 day" +%F)"
    through="$(simulate_through)"
    if [[ "$first" > "$through" ]]; then
        echo "Shop activity is in up to $last."
        return 0
    fi
    if [ "$runner" = dagster ]; then
        echo "Writing shop activity for $first to $through (one Dagster partition-range run)."
        dagster_env
        if ! "$REPO/.venv-dagster/bin/dagster" asset materialize \
                --working-directory "$REPO/dagster" -m dagster_crm.definitions \
                --select crm_source_daily_activity --partition-range "$first...$through" \
                >"$REPO/.local/simulate.log" 2>&1; then
            echo "crm_source_daily_activity failed for $first...$through. See .local/simulate.log" >&2
            return 1
        fi
        grep -oE "($keys) [0-9]{4}-[0-9]{2}-[0-9]{2}( is already in the CRM|: ).*" "$REPO/.local/simulate.log" \
            | sed 's/ \[crm_pipeline.*//; s/^/  /'
        return 0
    fi

    # A run of crm_daily_activity takes about 8 s to start, and the days since the seed grow by one
    # every day. So the last day is a run of the DAG, as the scheduler would make it, and the days
    # before it are written by the same helper in one process (python -m crm_pipeline.simulator),
    # as Dagster's partition-range run does: each brand's day, then its clickstream file.
    if [[ "$first" < "$through" ]]; then
        before="$(date -d "$through - 1 day" +%F)"
        echo "Writing shop activity for $first to $before (in one process), then $through (a crm_daily_activity run)."
        lakehouse_python -m crm_pipeline.simulator --from "$first" --to "$before" >"$REPO/.local/simulate.log" 2>&1 || {
            echo "Writing shop activity for $first to $before failed. See .local/simulate.log" >&2
            return 1
        }
        # The helper's lines, whether Airflow's logging wrote them (a time before, the logger after) or not
        grep -oE "($keys) [0-9]{4}-[0-9]{2}-[0-9]{2}( is already in the CRM|: ).*" "$REPO/.local/simulate.log" \
            | sed 's/ \[crm_pipeline.*//; s/^/  /'
    else
        echo "Writing shop activity for $through (a crm_daily_activity run)."
    fi
    # The 07:00 New York run on the next morning records the day
    logical="$(TZ=America/New_York date -d "$through + 1 day 07:00" --iso-8601=seconds)"
    airflow dags test crm_daily_activity "$logical" >"$REPO/.local/simulate.log" 2>&1 || {
        echo "crm_daily_activity failed for $through. See .local/simulate.log" >&2
        return 1
    }
    grep -oE "($keys) $through( is already in the CRM|: ).*" "$REPO/.local/simulate.log" \
        | sed 's/ \[crm_pipeline.*//; s/^/  /'
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

clickstream_history() {   # every day's clickstream file up to yesterday (or CRM_SIMULATE_UNTIL); existing files are kept
    local through
    through="$(simulate_through)"
    echo "Clickstream export: writing the days the landing volume lacks (the first time, the history since January 2025)."
    if [ -n "${CRM_SIMULATE_UNTIL:-}" ]; then
        lakehouse_python -m crm_pipeline.clickstream --to "$through"
    else
        lakehouse_python -m crm_pipeline.clickstream
    fi
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

    local_services_up
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

# --- ci-scenario: one fixed end-to-end run, the same on any day --------------------------------
# demo simulates up to yesterday, so its gold changes every day. The scenario stops on a fixed
# day, so its gold fingerprint can be compared with another run's: a pull request's with main's
# (.github/workflows/pipeline.yml). Under Airflow it runs the report on the seed (the first load,
# the clickstream history on Spark), writes three days of shop activity, and runs the report
# again (the incremental path, the three days in pandas) and the activation. Then Dagster runs
# its jobs on the same state, and the two orchestrators' gold must be the same.

CI_SCENARIO_END="2026-10-03"          # the last simulated day; the seed ends on SEED_END
CI_DIR="$REPO/.local/ci-scenario"     # each step's log, both fingerprints and summary.md

ci_note() {   # a line of the scenario's summary: on the terminal, and in summary.md for the workflow
    echo "$*"
    echo "$*" >> "$CI_DIR/summary.md"
}

ci_step() {   # ci_step <log name> <title> <command...>: the output goes to the log; a failure shows its end
    local name="$1" title="$2" started="$SECONDS" status
    shift 2
    echo "== $title"
    # In a subshell with its own set -e, and not as the condition of an `if`: there bash ignores
    # set -e inside the functions the step calls, so a failed run followed by any command passes
    set +e
    ( set -e; "$@" ) >"$CI_DIR/$name.log" 2>&1
    status=$?
    set -e
    CI_STEP_SECONDS=$((SECONDS - started))
    if [ "$status" != 0 ]; then
        echo "The end of .local/ci-scenario/$name.log:" >&2
        tail -n 40 "$CI_DIR/$name.log" >&2
        ci_note "- **FAILED: $title** (exit $status after $CI_STEP_SECONDS s; the log is \`$name.log\`)"
        return "$status"
    fi
}

ci_report_evidence() {   # ci_report_evidence <log name> <title> <engine the batch runs on, or ''> <changes|nothing: what the CDC finds>: what the log shows
    local log="$CI_DIR/$1.log" title="$2" engine="$3" captured="$4" freshness reconciled merges clickstream fx dbt problems=""
    local cdc brands cdc_found
    freshness="$(grep -c 'PASS freshness of' "$log" || true)"
    reconciled="$(grep -c 'rows in the source, .* in bronze' "$log" || true)"
    merges="$(grep -oE 'MERGE_[A-Z_]+ (succeeded|skipped)' "$log" | awk '{ print $2 }' | sort | uniq -c \
        | awk '{ printf "%s%s %s", (NR > 1 ? ", " : ""), $1, $2 }' || true)"
    clickstream="$(grep -oE 'Clickstream: (run|publish|nothing)[^.]*' "$log" | head -n 1 || true)"
    fx="$(grep -oE 'FX rates: (skipped|up to date|asked)[^.]*' "$log" | head -n 1 || true)"
    # One line per brand, as the capture logs it: "<brand> captured N changes" or "<brand> has nothing new"
    cdc="$(grep -oE 'CDC orders: [a-z0-9_]+ (captured [0-9]+ changes|has nothing new)' "$log" | sort -u || true)"
    brands="$(extracted_databases | wc -l | tr -d ' ')"
    dbt="$(grep -oE 'Done\. PASS=[0-9]+ WARN=[0-9]+ ERROR=[0-9]+' "$log" | tail -n 1 || true)"
    [ "$freshness" != 0 ] || problems="$problems no source freshness check passed;"
    [ "$reconciled" != 0 ] || problems="$problems no table was reconciled with the source;"
    [ -n "$merges" ] || problems="$problems no merge task ran;"
    [ -n "$clickstream" ] || problems="$problems the clickstream was not planned;"
    # The scenario has no ecb_fx_api connection: the FX side load must run, and say that it skips
    [[ "$fx" == "FX rates: skipped" ]] || problems="$problems the FX side load did not run and skip;"
    # The scenario keeps the crm_source_cdc connection, which is this machine's PostgreSQL: the
    # change data capture must have read every brand's slot. The three simulated days are changes
    # of every brand's orders; before them, and in a second run on the same state, there is none
    if [ "$captured" = changes ]; then
        cdc_found="$(grep -cE 'captured [1-9][0-9]* changes' <<<"$cdc" || true)"
        [ "$cdc_found" = "$brands" ] || problems="$problems the CDC did not capture changes of each of the $brands brands;"
        cdc="CDC: $(grep -oE 'captured [0-9]+' <<<"$cdc" | awk '{ sum += $2 } END { print sum + 0 }') changes of orders captured from the log, $cdc_found brands"
    else
        cdc_found="$(grep -c 'has nothing new' <<<"$cdc" || true)"
        if [ "$cdc_found" != "$brands" ] || grep -q 'captured' <<<"$cdc"; then
            problems="$problems the CDC did not read each of the $brands brands' slots and find nothing new;"
        fi
        cdc="CDC: nothing new in the log, $cdc_found brands"
    fi
    if [ -n "$engine" ] && ! grep -qE "Clickstream: run batch [^ ]+ on $engine" "$log"; then
        problems="$problems the clickstream batch did not run on $engine;"
    fi
    [[ "$dbt" == *" ERROR=0" ]] || problems="$problems dbt did not finish with ERROR=0;"
    # dagster-dbt's `--select fqn:*` on a command that is not a build: dbt checks nothing and passes
    ! grep -q 'Nothing to do' "$log" || problems="$problems a dbt command had nothing to do;"
    ci_note "- $title: $freshness freshness checks passed; $reconciled tables reconciled with the source;" \
        "merges: ${merges:-none}; ${clickstream:-no clickstream plan}; ${fx:-no FX side load}; $cdc;" \
        "dbt ${dbt:-did not finish} ($CI_STEP_SECONDS s)"
    if [ -n "$problems" ]; then
        ci_note "- **FAILED: $title did not do its work:**$problems see \`$1.log\`"
        return 1
    fi
}

ci_activation_evidence() {   # ci_activation_evidence <log name> <title>: the digest's day, and what became of the chart check
    local log="$CI_DIR/$1.log" title="$2" dashboard=""
    if grep -q 'All charts returned data' "$log"; then
        dashboard="every dashboard chart returned data"
    elif grep -q 'Superset is not running' "$log"; then
        dashboard="the dashboard check was skipped (Superset is not running)"
    fi
    # The digest reads gold: its day shows that gold ends where the scenario does
    if ! grep -qE "(Executive digest for |'as_of_date': ')$CI_SCENARIO_END" "$log" || [ -z "$dashboard" ]; then
        ci_note "- **FAILED: $title:** no KPI digest as of $CI_SCENARIO_END, or no dashboard check, in \`$1.log\`"
        return 1
    fi
    ci_note "- $title: the KPI digest is as of $CI_SCENARIO_END; $dashboard ($CI_STEP_SECONDS s)"
}

landing_days_after() {   # landing_days_after <day>: the landing volume's day folders after that day, one per line
    local landing="$CRM_LAKEHOUSE_ROOT/Volumes/${CRM_LAKEHOUSE_CATALOG:-local}/bronze/landing/web_events" folder
    for folder in "$landing"/*/; do
        [ -d "$folder" ] || continue
        folder="$(basename "$folder")"
        if [[ "$folder" > "$1" ]]; then echo "$folder"; fi
    done
}

ci_dagster_report() {   # the Dagster half's report: the side loads and clickstream jobs, then the report job
    rm -f "$ORCHESTRATOR_LOCK"      # this script's own claim, as Airflow: its DAG runs have ended
    claim_orchestrator dagster
    dagster_env
    prepare_dagster_manifest
    run_report dagster
}

ci_scenario() {   # ci_scenario [--reset]
    local reset=0 name pids target latest later catalog="${CRM_LAKEHOUSE_CATALOG:-local}" started="$SECONDS" parity=0
    case "${1:-}" in
        "") ;;
        --reset) reset=1 ;;
        *) echo "usage: $0 ci-scenario [--reset]" >&2; exit 2 ;;
    esac
    # Only on the emulators: the scenario resets what it loads into, and writes shop activity
    target="$(PYTHONPATH="$REPO/dags" "$REPO/.venv-dbt/bin/python" -m crm_pipeline.dbt_env | sed -n 's/^DBT_TARGET=//p')"
    if [ "$target" != emulator ] || [ -n "${AIRFLOW_CONN_DATABRICKS_DEFAULT:-}" ]; then
        echo "ci-scenario runs on the local emulators only: snowflake_default must point at fakesnow, and no" >&2
        echo "databricks_default connection may be set (.env.local.example has the local connections)." >&2
        exit 1
    fi
    # It runs the DAGs and the jobs itself, one orchestrator after the other, on the state they share
    for name in airflow dagster; do
        pids="$(orchestrator_pids "$(orchestrator_pattern "$name")")"
        if [ -n "$pids" ]; then
            echo "${name^} is running (PID $(pipe_list "$pids")). Stop it first: scripts/local.sh stop $name" >&2
            exit 1
        fi
    done
    claim_orchestrator airflow
    rm -rf "$CI_DIR"
    mkdir -p "$CI_DIR"
    # The scenario is fixed, and needs nothing outside this machine. With its connection the FX
    # side load would ask the ECB for the days since the seed was written, later than any order
    # of the scenario: gold would be the same, and CI would depend on a server it does not run.
    # Without it the load skips, and the rates are the seed's (dbt/seeds/fx_rates_ecb.csv).
    unset AIRFLOW_CONN_ECB_FX_API
    # The other side load stays on: crm_source_cdc is this machine's PostgreSQL. The slots are
    # created with the source databases, so the three simulated days are read from the log.

    if [ "$reset" = 1 ]; then
        echo "Resetting the source databases, the warehouse and the lakehouse ($CRM_LAKEHOUSE_ROOT: the"
        echo "$catalog catalog's tables and its landing files)."
        source_db_reset
        warehouse_reset
        rm -rf "${CRM_LAKEHOUSE_ROOT:?}/$catalog" "${CRM_LAKEHOUSE_ROOT:?}/Volumes/$catalog"
    else
        source_db_up
        latest="$(simulated_days | sort | tail -n 1)"
        later="$(landing_days_after "$CI_SCENARIO_END" | tail -n 1)"
        if [[ "$latest" > "$CI_SCENARIO_END" ]] || [ -n "$later" ]; then
            echo "The scenario ends on $CI_SCENARIO_END, and this machine holds later days: shop activity up to" >&2
            echo "$latest in the source databases${later:+, clickstream files up to $later in the landing volume}." >&2
            echo "It did not run. To run it from the seed: scripts/local.sh ci-scenario --reset" >&2
            echo "(that removes the simulated days, the warehouse and the lakehouse; afterwards" >&2
            echo "scripts/local.sh airflow demo writes them again, up to yesterday)." >&2
            exit 1
        fi
        if [ "$SOURCE_REBUILT" = 1 ]; then
            warehouse_reset
        else
            warehouse_up
        fi
    fi

    ci_note "### The fixed scenario: the seed (up to $SEED_END), then $(date -d "$SEED_END + 1 day" +%F) to $CI_SCENARIO_END"
    ci_note ""
    if [ -n "${CRM_LAKEHOUSE_LOCAL_WORKSPACE:-}" ]; then
        ci_note "- Spark batches run the job deployed to the local workspace ($CRM_LAKEHOUSE_LOCAL_WORKSPACE)"
    else
        ci_note "- Spark batches run from the source tree in .venv-spark"
    fi

    export CRM_SIMULATE_UNTIL="$SEED_END"
    ci_step clickstream-history "The clickstream history, up to $SEED_END" clickstream_history
    ci_note "- $(tail -n 1 "$CI_DIR/clickstream-history.log" | sed 's/, in \/.*//') ($CI_STEP_SECONDS s)"
    ci_step airflow-report-first "Airflow: the report on the seed (the first load, the clickstream history on Spark)" \
        run_report airflow
    ci_report_evidence airflow-report-first "Airflow, first load" spark nothing

    export CRM_SIMULATE_UNTIL="$CI_SCENARIO_END"
    ci_step simulate "Shop activity for $(date -d "$SEED_END + 1 day" +%F) to $CI_SCENARIO_END" simulate_days airflow
    ci_note "- Shop activity written: $(grep -c ' new customers ' "$CI_DIR/simulate.log" || true) brand-days ($CI_STEP_SECONDS s)"
    ci_step airflow-report-incremental "Airflow: the report again (the incremental path, the three days in pandas)" \
        run_report airflow
    ci_report_evidence airflow-report-incremental "Airflow, incremental" pandas changes
    ci_step dashboard-import "The Superset bundle: build and import" dashboard_import
    ci_note "- Dashboard: $(grep -oE '[0-9]+ datasets, [0-9]+ charts' "$CI_DIR/dashboard-import.log" | tail -n 1) imported ($CI_STEP_SECONDS s)"
    ci_step airflow-activation "Airflow: the activation (the KPI digest, the dashboard check)" run_activation airflow
    ci_activation_evidence airflow-activation "Airflow, activation"
    "$REPO/.venv/bin/python" "$REPO/local/gold_fingerprint.py" "$CI_DIR/gold-airflow.json" >/dev/null

    ci_step dagster-report "Dagster: the clickstream and report jobs on the same state" ci_dagster_report
    ci_report_evidence dagster-report "Dagster, report" "" nothing
    ci_step dagster-activation "Dagster: the activation job" run_activation dagster
    ci_activation_evidence dagster-activation "Dagster, activation"
    "$REPO/.venv-dagster/bin/python" "$REPO/local/gold_fingerprint.py" "$CI_DIR/gold-dagster.json" >/dev/null

    ci_note ""
    "$REPO/.venv/bin/python" "$REPO/local/gold_diff.py" --parity --markdown \
        --names Airflow Dagster "$CI_DIR/gold-airflow.json" "$CI_DIR/gold-dagster.json" >"$CI_DIR/parity.md" || parity=$?
    cat "$CI_DIR/parity.md"
    cat "$CI_DIR/parity.md" >> "$CI_DIR/summary.md"
    ci_note ""
    ci_note "The scenario took $((SECONDS - started)) s. Its logs and both fingerprints are in .local/ci-scenario/."
    if [ "$parity" != 0 ]; then
        echo "Airflow and Dagster built different gold from the same data: see the differences above." >&2
        exit 1
    fi
}

ci_summary() {   # ci_summary [<base fingerprint>]: the scenario's summary, then its gold against the base's, as Markdown
    local base="${1:-}" status=0
    if [ ! -f "$CI_DIR/summary.md" ]; then
        echo "The scenario did not start: there is no .local/ci-scenario/summary.md."
        return 0
    fi
    cat "$CI_DIR/summary.md"
    echo
    echo "### Gold against main"
    echo
    if [ ! -f "$CI_DIR/gold-airflow.json" ]; then
        echo "The scenario did not get as far as its gold fingerprint."
    elif [ -z "$base" ]; then
        echo "Not compared: this run is not a pull request."
    elif [ ! -f "$base" ]; then
        echo "No base to compare with yet: no successful run of this workflow on \`main\` has a \`gold-fingerprint\` artifact."
    else
        # A data difference is reported, never failed: it may be what the pull request is for
        "$REPO/.venv/bin/python" "$REPO/local/gold_diff.py" --markdown --names main "this change" \
            "$base" "$CI_DIR/gold-airflow.json" || status=$?
        [ "$status" -le 1 ] || return "$status"
    fi
}

# --- lint: what the workflow's lint job runs, with the same pinned tools -----------------------

changed_files() {   # changed_files <base commit> <pathspec...>: the files that differ from the base, and the new ones
    local base="$1"
    shift
    {
        git -C "$REPO" diff --name-only --diff-filter=ACMR "$base" -- "$@"
        git -C "$REPO" ls-files --others --exclude-standard -- "$@"
    } | sort -u
}

ruff_findings() {   # ruff_findings <folder> <files...>: "<file> <rule>" per finding, sorted. No line numbers: lines move
    local folder="$1"
    shift
    (cd "$folder" && uv tool run "ruff@$RUFF_VERSION" check --isolated --select F --target-version py312 --no-cache \
        --output-format concise --exit-zero -- "$@") \
        | sed -nE 's/^([^:]+):[0-9]+:[0-9]+: (F[0-9]+).*/\1 \2/p' | sort
}

lint_python() {   # ruff's F rules: no finding in a changed file that its base version does not have
    local base="$1" scratch="$2" file new
    local -a files=() in_base=()
    mapfile -t files < <(changed_files "$base" '*.py')
    if [ "${#files[@]}" = 0 ]; then
        echo "ruff: no Python file changed."
        return 0
    fi
    # The repo has no F finding (the first version's six were removed), so any finding is new. The
    # base's copy of each changed file still says which were there, should one ever be accepted.
    for file in "${files[@]}"; do
        if git -C "$REPO" cat-file -e "$base:$file" 2>/dev/null; then
            mkdir -p "$scratch/base/$(dirname "$file")"
            git -C "$REPO" show "$base:$file" > "$scratch/base/$file"
            in_base+=("$file")
        fi
    done
    : > "$scratch/ruff-base.txt"
    if [ "${#in_base[@]}" != 0 ]; then
        ruff_findings "$scratch/base" "${in_base[@]}" > "$scratch/ruff-base.txt"
    fi
    ruff_findings "$REPO" "${files[@]}" > "$scratch/ruff-now.txt"
    new="$(comm -13 "$scratch/ruff-base.txt" "$scratch/ruff-now.txt")"
    if [ -n "$new" ]; then
        echo "ruff: new findings (file, rule) in the changed files:" >&2
        echo "  ${new//$'\n'/$'\n'  }" >&2
        echo "See them with: uv tool run ruff@$RUFF_VERSION check --select F --target-version py312 <file>" >&2
        return 1
    fi
    echo "ruff: no new finding in ${#files[@]} changed Python files."
}

lint_sql() {   # sqlfluff with the dbt templater (dbt/.sqlfluff), on the new and changed models and tests only
    local base="$1" scratch="$2" port server status
    local -a files=() dbt_run=()
    mapfile -t files < <(changed_files "$base" 'dbt/models/*.sql' 'dbt/tests/*.sql')
    if [ "${#files[@]}" = 0 ]; then
        echo "sqlfluff: no dbt model or test changed."
        return 0
    fi
    requirements_section fakesnow > "$scratch/fakesnow.txt"
    requirements_section dbt > "$scratch/dbt.txt"
    # The templater compiles each model with dbt, which opens a connection. It gets an empty
    # emulator of its own on a free port, so the lint never touches the warehouse.
    port="$(uv run --no-project --python "$PYTHON_VERSION" python -c \
        'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"
    uv run --no-project --python "$PYTHON_VERSION" --with-requirements "$scratch/fakesnow.txt" \
        python "$REPO/local/fakesnow_server.py" "$scratch/fakesnow" "$port" >"$scratch/fakesnow.log" 2>&1 &
    server=$!
    for _ in $(seq 1 120); do
        if grep -q READY "$scratch/fakesnow.log" 2>/dev/null; then break; fi
        sleep 1
    done
    if ! grep -q READY "$scratch/fakesnow.log" 2>/dev/null; then
        echo "sqlfluff: the emulator for the dbt templater did not start:" >&2
        tail -n 20 "$scratch/fakesnow.log" >&2
        # shellcheck disable=SC2046
        kill $(process_tree "$server") 2>/dev/null || true
        return 1
    fi
    # The emulator is stopped whatever sqlfluff says, so its status is kept, not left to set -e
    # dbt's environment and packages, with its target and logs kept out of dbt/
    dbt_run=(env DBT_PROFILES_DIR="$REPO/dbt" DBT_TARGET=emulator SNOWFLAKE_PORT="$port"
             DBT_TARGET_PATH="$scratch/dbt-target" DBT_LOG_PATH="$scratch/dbt-logs" DBT_SEND_ANONYMOUS_USAGE_STATS=false
             uv run --no-project --python "$PYTHON_VERSION" --with-requirements "$scratch/dbt.txt")
    set +e
    (
        set -e
        cd "$REPO/dbt"
        if [ ! -d dbt_packages/dbt_utils ]; then
            "${dbt_run[@]}" dbt deps
        fi
        "${dbt_run[@]}" --with "sqlfluff==$SQLFLUFF_VERSION" --with "sqlfluff-templater-dbt==$SQLFLUFF_VERSION" \
            sqlfluff lint "${files[@]#dbt/}"
    )
    status=$?
    set -e
    # shellcheck disable=SC2046
    kill $(process_tree "$server") 2>/dev/null || true
    if [ "$status" != 0 ]; then
        echo "sqlfluff: findings in the changed dbt files (the rules are in dbt/.sqlfluff)." >&2
        return "$status"
    fi
    echo "sqlfluff: ${#files[@]} changed dbt files pass."
}

LINT_SCRATCH=""

lint() {   # lint [<base commit>]: compare with that commit (default HEAD: the uncommitted changes and new files)
    local base="${1:-HEAD}"
    if ! git -C "$REPO" cat-file -e "$base^{commit}" 2>/dev/null; then
        echo "lint: '$base' is not a commit here; comparing with HEAD instead."
        base=HEAD
    fi
    # Each check stops the command when it fails (set -e), so the folder is removed on the way out
    LINT_SCRATCH="$(mktemp -d)"
    trap 'rm -rf "$LINT_SCRATCH"' EXIT
    bash -n "$REPO/scripts/local.sh"
    echo "bash -n: scripts/local.sh parses."
    uv tool run --from "shellcheck-py==$SHELLCHECK_VERSION" shellcheck "$REPO/scripts/local.sh"
    echo "shellcheck: scripts/local.sh is clean."
    # actionlint runs shellcheck on each `run:` script when it finds one on the PATH, which a
    # GitHub runner has and a laptop may not. --with gives it the pinned one everywhere. SC2012
    # (ls instead of find) is lakehouse.yml's release step, left as it is.
    (cd "$REPO" && uv tool run --from "actionlint-py==$ACTIONLINT_VERSION" --with "shellcheck-py==$SHELLCHECK_VERSION" \
        actionlint -pyflakes= -ignore 'SC2012:' .github/workflows/*.yml)
    echo "actionlint: the workflows are clean."
    lint_python "$base" "$LINT_SCRATCH"
    lint_sql "$base" "$LINT_SCRATCH"
}

superset_ensure() {   # a Superset for one command: starts one when none answers (superset_release stops it again)
    if answers "http://127.0.0.1:$SUPERSET_PORT/health"; then
        return 0
    fi
    superset_start
    wait_for Superset "http://127.0.0.1:$SUPERSET_PORT/health" 180 .local/superset.log
}

superset_release() {   # stop the Superset superset_ensure started; one that was running before keeps running
    if [ -n "$FULLRUN_SUPERSET" ]; then
        # shellcheck disable=SC2046
        kill $(process_tree "$FULLRUN_SUPERSET") 2>/dev/null || true
        FULLRUN_SUPERSET=""
        echo "Superset stopped."
    fi
}

dashboard_check() {   # every chart through the Superset API (check_dashboard.py); starts Superset for it when none runs
    local status=0
    superset_ensure || status=$?
    if [ "$status" = 0 ]; then
        SUPERSET_USERNAME="admin" "$REPO/.venv-superset/bin/python" "$REPO/local/superset/check_dashboard.py" || status=$?
    fi
    superset_release
    return "$status"
}

# --- docs and site: what the pipeline built, as static pages in .local/site ---------------------

SITE_DIR="$REPO/.local/site"

dbt_docs() {   # the dbt docs as one HTML file, .local/site/dbt/index.html: models, columns, tests, lineage to the dashboard
    local exports
    exports="$(snowflake_env /dev/stdout)"
    # shellcheck disable=SC1090
    source <(printf '%s\n' "$exports")
    if [ "$DBT_TARGET" = emulator ]; then
        warehouse_up        # dbt reads the tables' columns from the warehouse for its catalog
    fi
    # In a target folder of its own: the orchestrators' builds keep theirs (target/airflow, target/dagster)
    (cd "$REPO/dbt" && DBT_PROFILES_DIR="$REPO/dbt" "$DBT_BIN" docs generate --static --target-path target/docs)
    mkdir -p "$SITE_DIR/dbt"
    cp "$REPO/dbt/target/docs/static_index.html" "$SITE_DIR/dbt/index.html"
    echo "dbt docs: .local/site/dbt/index.html (one self-contained file)"
}

site_build() {   # .local/site: index.html, the dbt docs, a picture of the dashboard, the diagram, gold's row counts
    local status=0
    local -a browser=(env PLAYWRIGHT_BROWSERS_PATH="$REPO/.local/playwright" uv run --no-project
                      --python "$PYTHON_VERSION" --with "playwright==$PLAYWRIGHT_VERSION" python)
    # CRM_SITE_BROWSER_DEPS=true: Playwright also installs the system libraries its Chromium needs,
    # with the system's package manager. That takes root: the workflow's runner and the dev
    # container allow it, a laptop would ask for a password, so it is never the default.
    local -a install=(install --only-shell chromium)
    if [ "${CRM_SITE_BROWSER_DEPS:-}" = true ]; then
        install=(install --with-deps --only-shell chromium)
    fi
    # The dbt docs draw the dashboard as the last node of the lineage, with a link to it. On a
    # site that is published, Superset's address on this machine leads nowhere: the site's docs
    # link to the site's own picture of the dashboard. `scripts/local.sh docs` keeps Superset's.
    CRM_EXPOSURE_URL="../dashboard.png" dbt_docs
    superset_ensure || status=$?
    if [ "$status" = 0 ]; then
        "${browser[@]}" -m playwright "${install[@]}" || status=$?
    fi
    if [ "$status" = 0 ]; then
        SUPERSET_USERNAME="admin" "${browser[@]}" "$REPO/local/site/capture_dashboard.py" "$SITE_DIR/dashboard.png" \
            || status=$?
    fi
    superset_release
    if [ "$status" != 0 ]; then
        echo "The dashboard's picture was not taken, so the site was not built." >&2
        return "$status"
    fi
    "$REPO/.venv/bin/python" "$REPO/local/site/build_site.py" "$SITE_DIR"
    # Readable by everyone, as a web server needs: under this script's umask the files were
    # their owner's only, and the archive the workflow's pages job publishes keeps each file's
    # mode. Nothing in the folder is a secret: the picture, the docs, the diagram, counts and sums.
    chmod -R a+rX "$SITE_DIR"
    echo "Open .local/site/index.html in a browser."
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
        pipeline_services
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
                pipeline_services
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
        case "${2:-}" in
            "")
                dashboard_import
                echo "Dashboard imported: http://localhost:$SUPERSET_PORT/superset/dashboard/executive-report/"
                ;;
            check) dashboard_check ;;
            *) echo "usage: $0 dashboard [check]" >&2; exit 2 ;;
        esac
        ;;
    docs)
        dbt_docs
        ;;
    site)
        site_build
        ;;
    ci-scenario)
        ci_scenario "${2:-}"
        ;;
    ci-summary)
        ci_summary "${2:-}"
        ;;
    lint)
        lint "${2:-}"
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
        # The replication slots the change data capture reads: a slot far behind keeps log files,
        # and one that is "lost" was invalidated and must be created again
        if [ "$("$e" container inspect -f '{{.State.Running}}' crm-source 2>/dev/null)" = "true" ]; then
            echo "CDC slots:  $(source_slots 2>/dev/null || echo "not readable")"
        else
            echo "CDC slots:  not shown (PostgreSQL is not running)"
        fi
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
        case "${1:-help}" in
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
            help|-h|--help)
                usage
                exit 0
                ;;
        esac
        # A mistyped command must not pass for a run that did its work (in CI, a step would be green)
        echo "Unknown command: $1" >&2
        usage >&2
        exit 2
        ;;
esac
