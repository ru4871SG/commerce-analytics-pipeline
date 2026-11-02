"""dbt's environment variables, computed from the snowflake_default connection.
"""
from __future__ import annotations

import os
import shlex
import sys

from crm_pipeline.config import ANALYTICS_DATABASE, BRONZE_DATABASE, DBT_PROJECT_DIR, SNOWFLAKE_CONN_ID
from crm_pipeline.connections import Connection, get_connection

SECRETS = {"SNOWFLAKE_PASSWORD", "SNOWFLAKE_PRIVATE_KEY", "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"}


def dbt_environment(conn: Connection | None = None) -> dict[str, str]:
    """The variables dbt/profiles.yml reads, for this connection."""
    conn = conn or get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra
    host = extra.get("host", "") or ""
    port = extra.get("port", "") or (os.environ.get("FAKESNOW_PORT", "64616") if host else "")
    return {
        "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
        "DBT_TARGET": os.environ.get("DBT_TARGET")
        or os.environ.get("AIRFLOW_VAR_CRM_DBT_TARGET")
        or ("emulator" if host else "prod"),
        "DBT_DEV_SCHEMA": os.environ.get("DBT_DEV_SCHEMA")
        or os.environ.get("AIRFLOW_VAR_CRM_DBT_DEV_SCHEMA", "dbt_dev"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        "SNOWFLAKE_ACCOUNT": str(extra.get("account", "") or ""),
        "SNOWFLAKE_HOST": str(host),
        "SNOWFLAKE_PORT": str(port),
        "SNOWFLAKE_PROTOCOL": str(extra.get("protocol", "") or ""),
        "SNOWFLAKE_USER": conn.login or "",
        "SNOWFLAKE_PASSWORD": conn.password or "",
        "SNOWFLAKE_PRIVATE_KEY": str(extra.get("private_key_content", "") or ""),
        "SNOWFLAKE_PRIVATE_KEY_PASSPHRASE": str(extra.get("private_key_passphrase", "") or ""),
        "SNOWFLAKE_ROLE": str(extra.get("role", "TRANSFORMER") or "TRANSFORMER"),
        "SNOWFLAKE_WAREHOUSE": str(extra.get("warehouse", "TRANSFORMING_WH") or "TRANSFORMING_WH"),
        "SNOWFLAKE_DATABASE": ANALYTICS_DATABASE,
        "BRONZE_DATABASE": BRONZE_DATABASE,
    }


def build_args(target: str) -> list[str]:
    """`dbt build`'s arguments for a target. fakesnow cannot create the temporary tables dbt's
    unit tests use, so emulator builds exclude them; they still run on real Snowflake."""
    return ["build"] + (["--exclude-resource-type", "unit_test"] if target == "emulator" else [])


def shell_exports(environment: dict[str, str]) -> str:
    return "".join(f"export {key}={shlex.quote(value)}\n" for key, value in environment.items())


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    environment = dbt_environment()
    if argv[:1] == ["--shell"] and len(argv) == 2:
        with open(argv[1], "w") as out:
            out.write(shell_exports(environment))
        return 0
    for key, value in environment.items():
        print(f"{key}={'<redacted>' if key in SECRETS and value else value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
