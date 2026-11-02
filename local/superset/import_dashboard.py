#!/usr/bin/env python3
"""Import local/superset/executive_report.zip into Superset, replacing what an earlier import made.

    .venv-superset/bin/python local/superset/import_dashboard.py

`superset import-dashboards` overwrites only the dashboard: the database, datasets and charts
it imported before are kept as they were, so a changed metric or chart in build_bundle.py would
never arrive. This uses Superset's assets importer instead, which overwrites every object in the
bundle (matched by UUID).

The bundle keeps the database password masked; it is passed here (SNOWFLAKE_PASSWORD, `snow` for
local fakesnow). Finally the import has mapped chart UUIDs to numeric IDs, so the native Brand
filter's `chartsInScope` is set to every chart on the dashboard.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from zipfile import ZipFile

from flask import g
from superset.app import create_app

HERE = Path(__file__).resolve().parent
BUNDLE = HERE / "executive_report.zip"
SLUG = "executive-report"
DATABASE_FILE = "databases/snowflake_local.yaml"


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db, security_manager
        from superset.commands.importers.v1.assets import ImportAssetsCommand
        from superset.commands.importers.v1.utils import get_contents_from_bundle
        from superset.models.dashboard import Dashboard

        g.user = security_manager.find_user(username="admin")
        with ZipFile(BUNDLE) as bundle:
            contents = get_contents_from_bundle(bundle)
        ImportAssetsCommand(
            contents, passwords={DATABASE_FILE: os.environ.get("SNOWFLAKE_PASSWORD", "snow")},
        ).run()

        dashboard = db.session.query(Dashboard).filter_by(slug=SLUG).one()
        position = dashboard.position or {}
        chart_ids = [
            node["meta"]["chartId"]
            for node in position.values()
            if isinstance(node, dict) and node.get("type") == "CHART"
        ]
        metadata = json.loads(dashboard.json_metadata or "{}")
        for native_filter in metadata.get("native_filter_configuration", []):
            native_filter["chartsInScope"] = chart_ids
        global_config = metadata.setdefault("global_chart_configuration", {})
        global_config["chartsInScope"] = chart_ids
        dashboard.json_metadata = json.dumps(metadata)
        db.session.commit()
        print(f"Imported {len(contents)} files; Brand filter scoped to {len(chart_ids)} charts.")


if __name__ == "__main__":
    main()
