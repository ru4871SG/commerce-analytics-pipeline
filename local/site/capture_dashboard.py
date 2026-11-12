#!/usr/bin/env python3
"""Save a full-page PNG of the Executive Report dashboard, once every chart on it has drawn.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "superset"))
from safe_url import sends_password_in_clear  # noqa: E402

BASE_URL = os.environ.get("SUPERSET_URL", "http://127.0.0.1:8088").rstrip("/")
SLUG = "executive-report"
NOT_RUNNING = 99
INSECURE_URL = 2
LOGIN_FAILED = 3

# What each chart on the page looks like now: still loading, failed, or drew nothing
CHART_STATES = """() => [...document.querySelectorAll('[data-test-chart-id]')].map(chart => ({
    id: Number(chart.getAttribute('data-test-chart-id')),
    loading: chart.querySelector('[data-test="loading-indicator"], .loading') !== null,
    failed: chart.querySelector('[role="alert"]') !== null,
    empty: chart.querySelector('.ant-empty') !== null,
}))"""


def is_running() -> bool:
    try:
        with urllib.request.urlopen(f"{BASE_URL}/health", timeout=10):
            return True
    except (urllib.error.URLError, OSError):
        return False


def log_in(page, username: str, password: str, seconds: float = 30) -> bool:
    """Type the login into Superset's form. True once Superset has let the session in."""
    page.goto(f"{BASE_URL}/login/", wait_until="domcontentloaded")
    page.fill("#username", username)
    page.fill("#password", password)
    page.click("[data-test=login-button]")
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if "/login" not in page.url:
            return True
        page.wait_for_timeout(250)
    return False


def unfinished(states: list[dict], answered: dict[int, int], expected: dict[int, str]) -> list[str]:
    """The charts that have not finished: not on the page yet, not answered, or still loading."""
    on_page = {state["id"]: state for state in states}
    return [name for chart_id, name in expected.items()
            if chart_id not in on_page or chart_id not in answered or on_page[chart_id]["loading"]]


def problems(states: list[dict], answered: dict[int, int], expected: dict[int, str]) -> list[str]:
    """What is wrong with the finished charts: one line per chart Superset could not draw."""
    found = []
    for state in states:
        name = expected.get(state["id"], f"chart {state['id']}")
        if answered.get(state["id"], 200) != 200:
            found.append(f"{name}: its query answered HTTP {answered[state['id']]}")
        elif state["failed"]:
            found.append(f"{name}: Superset shows an error")
        elif state["empty"]:
            found.append(f"{name}: no results")
    return found


def capture(page, request, out: Path, width: int, timeout: float, prepare=None) -> int:
    """Open the dashboard, wait for its charts, save the picture. prepare(page) runs before the
    dashboard opens (the tests use it to make a chart fail)."""
    listed = request.get(f"{BASE_URL}/api/v1/dashboard/{SLUG}/charts")
    if not listed.ok:
        print(f"Superset has no dashboard '{SLUG}' (HTTP {listed.status}): run scripts/local.sh dashboard.",
              file=sys.stderr)
        return 1
    expected = {chart["id"]: chart["slice_name"] for chart in listed.json()["result"]}

    answered: dict[int, int] = {}

    def on_response(response) -> None:
        if "/api/v1/chart/data" not in response.url:
            return
        try:
            chart_id = json.loads(response.request.post_data or "{}").get("form_data", {}).get("slice_id")
        except ValueError:
            return
        if chart_id in expected:
            answered[chart_id] = response.status

    page.on("response", on_response)
    if prepare:
        prepare(page)
    # Without Superset's own menu, the Brand filter folded to its icon, so the charts get the width
    page.goto(f"{BASE_URL}/superset/dashboard/{SLUG}/?standalone=1&expand_filters=0", wait_until="domcontentloaded")
    page.wait_for_selector("[data-test-chart-id]", timeout=60_000)
    # The whole dashboard in the window, so the picture needs no resize
    page.set_viewport_size({"width": width, "height": page.evaluate("document.documentElement.scrollHeight")})

    deadline = time.monotonic() + timeout
    while True:
        states = page.evaluate(CHART_STATES)
        waiting = unfinished(states, answered, expected)
        if not waiting:
            break
        if time.monotonic() > deadline:
            print(f"{len(waiting)} of {len(expected)} charts had not finished after {timeout:.0f} s: "
                  + ", ".join(waiting), file=sys.stderr)
            return 1
        page.wait_for_timeout(500)

    page.wait_for_timeout(1500)     
    height = page.evaluate("document.documentElement.scrollHeight")
    if height != page.viewport_size["height"]:
        page.set_viewport_size({"width": width, "height": height})
        page.wait_for_timeout(1500)
    out.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(out), full_page=True)
    wrong = problems(page.evaluate(CHART_STATES), answered, expected)
    if wrong:
        print(f"Saved {out}, but {len(wrong)} of {len(expected)} charts did not draw:", file=sys.stderr)
        for line in wrong:
            print(f" - {line}", file=sys.stderr)
        return 1
    print(f"Saved {out} ({width} x {height} px): all {len(expected)} charts drawn.")
    return 0


def main(argv: list[str] | None = None, prepare=None) -> int:
    parser = argparse.ArgumentParser(description="Save a full-page PNG of the Executive Report dashboard.")
    parser.add_argument("out", type=Path)
    parser.add_argument("--width", type=int, default=1600)
    parser.add_argument("--color-scheme", choices=["dark", "light"], default="dark",
                        help="Superset follows the browser's preference (default: dark, as the README's pictures)")
    parser.add_argument("--timeout", type=float, default=300, help="seconds to wait for the charts")
    args = parser.parse_args(argv)

    if sends_password_in_clear(BASE_URL):
        print(f"Refusing to log in to {BASE_URL}: the password would cross the network unencrypted. "
              "Use an https:// SUPERSET_URL.", file=sys.stderr)
        return INSECURE_URL
    username = os.environ.get("SUPERSET_USERNAME", "admin")
    password = os.environ.get("SUPERSET_PASSWORD")
    if not is_running():
        print(f"Superset is not running at {BASE_URL}: nothing to capture.")
        return NOT_RUNNING
    if not password:
        print("SUPERSET_PASSWORD is not set. scripts/local.sh exports it from .env, which "
              "scripts/local.sh install creates.", file=sys.stderr)
        return LOGIN_FAILED

    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(viewport={"width": args.width, "height": 1000},
                                          color_scheme=args.color_scheme)
            page = context.new_page()
            if not log_in(page, username, password):
                print(f"Superset at {BASE_URL} refused the login of {username}: check SUPERSET_PASSWORD "
                      "in .env, and run scripts/local.sh install after changing it.", file=sys.stderr)
                return LOGIN_FAILED
            return capture(page, context.request, args.out, args.width, args.timeout, prepare)
        finally:
            browser.close()


if __name__ == "__main__":
    raise SystemExit(main())
