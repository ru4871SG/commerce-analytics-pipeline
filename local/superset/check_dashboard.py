#!/usr/bin/env python3
"""Smoke-test every chart of the imported Executive Report through the Superset API.

Run after `scripts/local.sh dashboard` and while Superset is running:

    .venv-superset/bin/python local/superset/check_dashboard.py

It logs in as admin with SUPERSET_PASSWORD (scripts/local.sh exports it from .env), sends one
query per chart to POST /api/v1/chart/data and checks that each returns rows. Then it checks the
dashboard's native Brand filter: that it targets brand_name on every dataset and covers every
chart, and that every chart filtered to one brand returns only that brand.

SUPERSET_URL, SUPERSET_USERNAME and SUPERSET_PASSWORD point it at another Superset. A Superset
off this machine must be reached over https: the password is never sent in clear text. When
Superset does not answer it exits with 99, which the crm_executive_activation DAG treats as
"skip the check". Without SUPERSET_PASSWORD, or when Superset refuses the login, it says so and
exits with 3: there is no default password, since install generates a random one.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_bundle import CHARTS, DATASETS, SLUG  # noqa: E402
from safe_url import sends_password_in_clear  # noqa: E402

BASE_URL = os.environ.get("SUPERSET_URL", "http://127.0.0.1:8088").rstrip("/")
NOT_RUNNING = 99
INSECURE_URL = 2
LOGIN_FAILED = 3
DATASET_BY_UUID = {definition["uuid"]: name for name, definition in DATASETS.items()}


def query_for(spec: dict) -> dict:
    params = spec["params"]
    viz = spec["viz_type"]
    columns: list[str] = []
    metrics: list[str]
    granularity = None
    is_timeseries = False

    if viz == "big_number_total":
        metrics = [params["metric"]]
    elif viz == "big_number":            # latest value with a daily trend line
        metrics = [params["metric"]]
        granularity = params["x_axis"]
        is_timeseries = True
    elif viz == "pivot_table_v2":
        columns = list(params.get("groupbyRows", [])) + list(params.get("groupbyColumns", []))
        metrics = list(params["metrics"])
    elif viz == "table" and params.get("query_mode") == "raw":      # the rows as they are stored
        columns = list(params["all_columns"])
        metrics = []
    elif viz == "mixed_timeseries":
        columns = list(params.get("groupby", []))
        metrics = list(params["metrics"]) + list(params["metrics_b"])
        granularity = params.get("x_axis")
        is_timeseries = True
    else:
        columns = list(params.get("groupby", []))
        metrics = list(params.get("metrics", []))
        x_axis = params.get("x_axis")
        if x_axis in ("date_day", "week_start", "month_start"):
            granularity = x_axis
            is_timeseries = True
        elif x_axis:
            columns.append(x_axis)
    # The chart's own filter is part of what it shows: a table of the as-of day must find that day
    where = " AND ".join(f"({item['sqlExpression']})" for item in params.get("adhoc_filters", [])
                         if item.get("clause") == "WHERE" and item.get("expressionType") == "SQL")
    return {
        "columns": columns,
        "metrics": metrics,
        "granularity": granularity,
        "is_timeseries": is_timeseries,
        "time_range": "No filter",
        "row_limit": 500,
        "orderby": [json.loads(item) for item in params.get("order_by_cols", [])],
        "extras": {"where": where} if where else {},
    }


def run_chart(session, headers, dataset_ids, chart_ids, spec, filters=None):
    """Send the chart's query to /api/v1/chart/data. Returns (rows, error)."""
    dataset_id = dataset_ids[DATASET_BY_UUID[spec["dataset_uuid"]]]
    query = query_for(spec)
    if filters:
        query["filters"] = filters
    payload = {
        "datasource": {"id": dataset_id, "type": "table"},
        "queries": [query],
        "result_type": "full",
        "result_format": "json",
        "form_data": {
            "viz_type": spec["viz_type"],
            "datasource": f"{dataset_id}__table",
            "slice_id": chart_ids[spec["slice_name"]],
        },
    }
    response = session.post(f"{BASE_URL}/api/v1/chart/data", headers=headers, json=payload)
    if response.status_code != 200:
        return None, f"HTTP {response.status_code} {response.text[:200]}"
    result = response.json().get("result", [{}])[0]
    if result.get("status") != "success":
        return None, result.get("error") or result.get("stacktrace")
    rows = result.get("data") or []
    return rows, None if rows else "no rows"


def check_native_filter(session, headers, dataset_ids) -> list[str]:
    """The dashboard's Brand filter must target brand_name on every dataset and cover every chart.
    The import replaces each target's datasetUuid with the dataset's numeric datasetId."""
    dashboard = session.get(f"{BASE_URL}/api/v1/dashboard/{SLUG}", headers=headers).json()["result"]
    position = json.loads(dashboard["position_json"])
    on_dashboard = {node["meta"]["chartId"] for node in position.values()
                    if isinstance(node, dict) and node.get("type") == "CHART"}
    filters = json.loads(dashboard["json_metadata"]).get("native_filter_configuration", [])
    brand = next((f for f in filters if f.get("name") == "Brand"), None)
    if brand is None:
        return ["Brand filter: not on the dashboard"]
    problems = []
    expected = {dataset_ids[name] for name in DATASETS}
    targeted = {t.get("datasetId") for t in brand["targets"] if t.get("column", {}).get("name") == "brand_name"}
    if targeted != expected:
        problems.append(f"Brand filter: targets {len(targeted & expected)} of {len(expected)} datasets on brand_name")
    missing = on_dashboard - set(brand.get("chartsInScope", []))
    if missing:
        problems.append(f"Brand filter: {len(missing)} of {len(on_dashboard)} charts are out of its scope")
    if not problems:
        print(f"  ok  Brand filter covers all {len(on_dashboard)} charts and {len(targeted)} datasets")
    return problems


def main() -> int:
    if sends_password_in_clear(BASE_URL):
        print(f"Refusing to log in to {BASE_URL}: the password would cross the network unencrypted. "
              "Use an https:// SUPERSET_URL.", file=sys.stderr)
        return INSECURE_URL
    session = requests.Session()
    username = os.environ.get("SUPERSET_USERNAME", "admin")
    password = os.environ.get("SUPERSET_PASSWORD")
    try:
        if password:
            login = session.post(f"{BASE_URL}/api/v1/security/login", json={
                "username": username, "password": password, "provider": "db", "refresh": True,
            }, timeout=30)
        else:
            session.get(f"{BASE_URL}/health", timeout=30)      # is there a Superset to check at all?
    except requests.ConnectionError:
        print(f"Superset is not running at {BASE_URL}: nothing to check.")
        return NOT_RUNNING
    if not password:
        print("SUPERSET_PASSWORD is not set. scripts/local.sh exports it from .env, which "
              "scripts/local.sh install creates.", file=sys.stderr)
        return LOGIN_FAILED
    if login.status_code == 401:
        print(f"Superset at {BASE_URL} refused the login of {username}: check SUPERSET_PASSWORD "
              "in .env, and run scripts/local.sh install after changing it.", file=sys.stderr)
        return LOGIN_FAILED
    login.raise_for_status()
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    datasets = session.get(f"{BASE_URL}/api/v1/dataset/", headers=headers,
                           params={"q": "(page_size:100)"}).json()["result"]
    dataset_ids = {row["table_name"]: row["id"] for row in datasets}

    charts = session.get(f"{BASE_URL}/api/v1/chart/", headers=headers,
                         params={"q": "(page_size:100)"}).json()["result"]
    chart_ids = {row["slice_name"]: row["id"] for row in charts}
    print(f"{len(charts)} charts, {len(datasets)} datasets")

    failures = []
    unfiltered = {}
    for spec in CHARTS:
        name = spec["slice_name"]
        if name not in chart_ids:
            failures.append(f"{name}: chart not imported")
            continue
        rows, error = run_chart(session, headers, dataset_ids, chart_ids, spec)
        if error:
            failures.append(f"{name}: {error}")
            continue
        unfiltered[name] = rows
        print(f"  ok  {name:48} {len(rows):4} rows")

    failures += check_native_filter(session, headers, dataset_ids)

    # Every chart with the Brand filter applied: grouped charts may only return that brand, and
    # a card must change (revenue YTD for one brand is less than for all three).
    brand_filter = [{"col": "brand_name", "op": "IN", "val": ["Broomx"]}]
    failures_before = len(failures)
    filtered = {}
    for spec in CHARTS:
        name = spec["slice_name"]
        if name not in unfiltered:
            continue
        rows, error = run_chart(session, headers, dataset_ids, chart_ids, spec, brand_filter)
        if error:
            failures.append(f"{name} (Brand = Broomx): {error}")
            continue
        filtered[name] = rows
        brands = {row["brand_name"] for row in rows if "brand_name" in row}
        if brands - {"Broomx"}:
            failures.append(f"{name} (Brand = Broomx): rows contain {sorted(brands)}")
    card = "Revenue YTD"
    if card in filtered:
        rows = filtered[card]
        if rows:
            one, every = (next(iter(r[0].values())) for r in (rows, unfiltered[card]))
            if not one < every:
                failures.append(f"{card}: Broomx alone ({one}) is not less than every brand ({every})")
    if len(failures) == failures_before:
        print("  ok  every chart filtered to one brand returns only that brand")

    if failures:
        print("\nFAILURES")
        for failure in failures:
            print(" -", failure)
        return 1
    print("\nAll charts returned data; the Brand filter is wired to every chart and filters them.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
