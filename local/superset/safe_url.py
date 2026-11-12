"""One rule for every script that logs in to Superset with superset password in env
"""
from __future__ import annotations

import ipaddress
from urllib.parse import urlsplit


def sends_password_in_clear(url: str) -> bool:
    """Whether logging in at this URL would send the password unencrypted over a network:
    plain http to a host other than this machine."""
    parts = urlsplit(url)
    if parts.scheme != "http":
        return False
    host = parts.hostname or ""
    if host == "localhost":
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return True
