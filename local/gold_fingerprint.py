#!/usr/bin/env python3
"""A fingerprint of ANALYTICS.GOLD: every exec_* table's row count and numeric sums.
"""
from __future__ import annotations

import json
import os
import sys
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def load_env() -> None:
    """Read .env, or the committed local example when there is none, like scripts/local.sh."""
    candidates = [REPO_ROOT / ".env", REPO_ROOT / ".env.local.example"]
    for path in candidates:
        if not path.exists():
            continue
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
                value = value[1:-1]
            os.environ.setdefault(key.strip(), value)
        break


def connection() -> dict:
    """The snowflake_default connection as connector keyword arguments."""
    load_env()
    raw = os.environ.get("AIRFLOW_CONN_SNOWFLAKE_DEFAULT")
    if not raw:
        raise SystemExit("AIRFLOW_CONN_SNOWFLAKE_DEFAULT is not set")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        from urllib.parse import parse_qs, unquote, urlparse

        parsed = urlparse(raw)
        payload = {
            "login": unquote(parsed.username or ""),
            "password": unquote(parsed.password or ""),
            "extra": {key: values[-1] for key, values in parse_qs(parsed.query).items()},
        }
    extra = payload.get("extra") or {}
    kwargs = {
        "user": payload.get("login"),
        "password": payload.get("password"),
        "account": extra.get("account", "fakesnow"),
        "database": extra.get("database", "ANALYTICS"),
        "warehouse": extra.get("warehouse"),
        "role": extra.get("role"),
    }
    if extra.get("host"):
        kwargs.update(
            host=extra["host"],
            port=int(extra.get("port", 443)),
            protocol=extra.get("protocol", "https"),
            socket_timeout=int(extra.get("socket_timeout", 900)),
        )
    return {key: value for key, value in kwargs.items() if value is not None}


def plain(value):
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else round(float(value), 6)
    if isinstance(value, float):
        return round(value, 6)
    return value


def gold_tables(cursor) -> list[str]:
    cursor.execute(
        "select table_name from ANALYTICS.information_schema.tables "
        "where table_schema = 'GOLD' and table_name like 'EXEC%' order by table_name"
    )
    return [row[0].upper() for row in cursor.fetchall()]


def fingerprint(cursor) -> dict:
    result = {}
    for table in gold_tables(cursor):
        cursor.execute(
            "select column_name, data_type from ANALYTICS.information_schema.columns "
            "where table_schema = 'GOLD' and table_name = %s order by ordinal_position",
            (table,),
        )
        numeric = [name for name, data_type in cursor.fetchall() if data_type.upper() in ("NUMBER", "FLOAT", "DECIMAL")]
        sums = ", ".join(f'sum("{column}") as "{column}"' for column in numeric) or "null as \"_none\""
        cursor.execute(f'select count(*), {sums} from ANALYTICS.GOLD."{table}"')
        row = cursor.fetchone()
        result[table.lower()] = {
            "rows": int(row[0]),
            "sums": {column.lower(): plain(value) for column, value in zip(numeric, row[1:])},
        }
    return result


def main() -> int:
    import snowflake.connector

    conn = snowflake.connector.connect(**connection())
    try:
        data = fingerprint(conn.cursor())
    finally:
        conn.close()
    text = json.dumps(data, indent=2, sort_keys=True)
    print(text)
    if len(sys.argv) > 1:
        Path(sys.argv[1]).write_text(text + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
