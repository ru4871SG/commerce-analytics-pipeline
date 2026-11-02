#!/usr/bin/env python3
"""Run the fakesnow Snowflake emulator as a file-backed server.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

from fakesnow import server as run_server
import fakesnow.server as server_module
from fakesnow.instance import FakeSnow


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit("usage: fakesnow_server.py <data-directory> <port>")
    data_dir = Path(sys.argv[1]).resolve()
    port = int(sys.argv[2])
    data_dir.mkdir(parents=True, exist_ok=True)

    server_module.shared_fs = FakeSnow(db_path=data_dir)
    cursor = server_module.shared_fs.connect(None, None).cursor()
    for database in ("raw", "analytics"):
        cursor.execute(f"create database if not exists {database}")
    cursor.close()

    with run_server(host="127.0.0.1", port=port):
        print("READY", flush=True)
        while True:
            time.sleep(3600)


if __name__ == "__main__":
    main()
