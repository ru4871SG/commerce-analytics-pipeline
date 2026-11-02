"""Connection lookup
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from urllib.parse import parse_qs, unquote, urlparse


class ConnectionNotFound(LookupError):
    """No connection with this id is configured."""


@dataclass(frozen=True)
class Connection:
    """The fields of an Airflow connection the pipeline uses."""

    conn_id: str
    conn_type: str = ""
    host: str | None = None
    port: int | None = None
    login: str | None = None
    password: str | None = None
    extra: dict = field(default_factory=dict)

    @property
    def extra_dejson(self) -> dict:
        """The old Airflow spelling, kept so call sites read the same as before."""
        return self.extra


def env_var_name(conn_id: str) -> str:
    return f"AIRFLOW_CONN_{conn_id.upper()}"


def _optional_port(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _extra(value) -> dict:
    if value is None or value == "":
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _from_uri(conn_id: str, raw: str) -> Connection:
    parsed = urlparse(raw)
    if not parsed.scheme:
        raise ValueError(f"{env_var_name(conn_id)} is neither a connection URI nor JSON")
    # Airflow puts URI query parameters in the connection's extra.
    extra = {key: values[-1] for key, values in parse_qs(parsed.query, keep_blank_values=True).items()}
    return Connection(
        conn_id=conn_id,
        conn_type=parsed.scheme,
        host=parsed.hostname or None,
        port=_optional_port(parsed.port),
        login=unquote(parsed.username) if parsed.username is not None else None,
        password=unquote(parsed.password) if parsed.password is not None else None,
        extra=extra,
    )


def _from_json(conn_id: str, raw: str) -> Connection:
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError(f"{env_var_name(conn_id)} must be a JSON object")
    extra = _extra(payload.get("extra"))
    # Airflow also accepts extra keys at the top level of the JSON object.
    for key, value in payload.items():
        if key not in {"conn_type", "host", "port", "login", "password", "schema", "extra"}:
            extra.setdefault(key, value)
    return Connection(
        conn_id=conn_id,
        conn_type=payload.get("conn_type") or "",
        host=payload.get("host") or None,
        port=_optional_port(payload.get("port")),
        login=payload.get("login") or None,
        password=payload.get("password") or None,
        extra=extra,
    )


def parse_connection(conn_id: str, raw: str) -> Connection:
    """Parse one AIRFLOW_CONN_* value, in the two formats Airflow accepts."""
    raw = raw.strip()
    if raw.startswith("{"):
        return _from_json(conn_id, raw)
    return _from_uri(conn_id, raw)


def get_connection(conn_id: str) -> Connection:
    """The connection as Airflow would resolve it, or from AIRFLOW_CONN_<CONN_ID> without Airflow.

    Raises ConnectionNotFound when there is no such connection.
    """
    try:
        from airflow.sdk import BaseHook
        from airflow.sdk.exceptions import AirflowNotFoundException
    except ImportError:
        BaseHook = None
    else:
        try:
            conn = BaseHook.get_connection(conn_id)
        except AirflowNotFoundException as exc:
            raise ConnectionNotFound(str(exc)) from exc
        return Connection(
            conn_id=conn_id,
            conn_type=conn.conn_type or "",
            host=conn.host or None,
            port=_optional_port(conn.port),
            login=conn.login or None,
            password=conn.password or None,
            extra=dict(conn.extra_dejson),
        )

    raw = os.environ.get(env_var_name(conn_id))
    if not raw:
        raise ConnectionNotFound(f"The conn_id `{conn_id}` isn't defined ({env_var_name(conn_id)} is not set)")
    return parse_connection(conn_id, raw)


def airflow_installed() -> bool:
    """Whether the Airflow packages can be imported, without importing them."""
    try:
        import airflow  # noqa: F401
    except ImportError:
        return False
    return True
