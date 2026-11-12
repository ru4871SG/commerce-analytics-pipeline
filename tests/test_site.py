"""The local site."""
from __future__ import annotations

import importlib.util
import re
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = (REPO / "scripts" / "local.sh").read_text()


def load(name: str):
    spec = importlib.util.spec_from_file_location(name, REPO / "local" / "site" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def capture():
    before = list(sys.path)
    yield load("capture_dashboard")      # puts local/superset on sys.path to import safe_url
    sys.path[:] = before


@pytest.fixture(scope="module")
def site():
    before = list(sys.path)
    yield load("build_site")             # puts local/ on sys.path to import gold_fingerprint
    sys.path[:] = before


CHARTS = {1: "Revenue MTD", 2: "Revenue by week", 3: "Rebuy %"}


def state(chart_id: int, **flags) -> dict:
    return {"id": chart_id, "loading": False, "failed": False, "empty": False, **flags}



def test_a_chart_has_not_finished_until_it_is_on_the_page_answered_and_no_longer_loading(capture):
    states = [state(1), state(2, loading=True)]  
    assert capture.unfinished(states, {1: 200, 2: 200, 3: 200}, CHARTS) == ["Revenue by week", "Rebuy %"]
    assert capture.unfinished([state(1), state(2), state(3)], {1: 200, 3: 200}, CHARTS) == ["Revenue by week"]
    assert capture.unfinished([state(1), state(2), state(3)], {1: 200, 2: 200, 3: 200}, CHARTS) == []


def test_a_failed_query_an_error_and_an_empty_chart_each_fail_the_capture(capture):
    states = [state(1), state(2, failed=True), state(3, empty=True)]
    assert capture.problems(states, {1: 500, 2: 200, 3: 200}, CHARTS) == [
        "Revenue MTD: its query answered HTTP 500",
        "Revenue by week: Superset shows an error",
        "Rebuy %: no results",
    ]
    assert capture.problems([state(1), state(2), state(3)], {1: 200, 2: 200, 3: 200}, CHARTS) == []


def test_the_capture_uses_the_exit_codes_of_the_dashboard_check(capture):
    source = (REPO / "local" / "superset" / "check_dashboard.py").read_text()
    for name in ("NOT_RUNNING", "INSECURE_URL", "LOGIN_FAILED"):
        assert f"{name} = {getattr(capture, name)}\n" in source


# Index Page
FINGERPRINT = {
    "exec_web_daily_sessions": {"rows": 12063, "sums": {"sessions": 1.0}},
    "exec_kpi_snapshot": {"rows": 3, "sums": {}},
}


def test_the_index_page_shows_the_as_of_date_and_each_tables_rows_and_links_to_the_rest(site):
    page = site.render(date(2026, 11, 9), FINGERPRINT, "eb40f48", datetime(2026, 11, 10, 13, 44, tzinfo=timezone.utc))
    assert "Gold is as of <strong>2026-11-09</strong>" in page
    assert "built 2026-11-10 13:44 UTC from commit <code>eb40f48</code>" in page
    assert '<tr><td><code>exec_web_daily_sessions</code></td><td class="number">12,063</td></tr>' in page
    assert page.index("exec_kpi_snapshot") < page.index("exec_web_daily_sessions")       # in name order
    for target in ('href="dbt/index.html"', 'src="dashboard.png"', 'src="pipeline.svg"',
                   'href="gold-fingerprint.json"'):
        assert target in page
    assert "http://" not in page and "https://" not in page     # nothing is fetched from elsewhere


def test_a_page_built_on_github_links_to_the_repository_and_still_fetches_nothing(site):
    assert site.repository_url({}) is None
    address = site.repository_url({"GITHUB_REPOSITORY": "ru4871SG/commerce-analytics-pipeline",
                                   "GITHUB_SERVER_URL": "https://github.com"})
    assert address == "https://github.com/ru4871SG/commerce-analytics-pipeline"
    page = site.render(date(2026, 10, 3), FINGERPRINT, "eb40f48", datetime(2026, 11, 11, tzinfo=timezone.utc), address)
    assert f'<a href="{address}">{address}</a>' in page
    assert not re.search(r'src="https?://', page) and page.count("https://") == 2 and "http://" not in page
    assert '"><script>' not in site.render(None, {}, "c", datetime.now(timezone.utc), 'https://x/"><script>')


def test_the_index_page_escapes_what_it_is_given_and_says_when_there_is_no_as_of_date(site):
    page = site.render(None, {"<b>table</b>": {"rows": 1, "sums": {}}}, "<script>", datetime.now(timezone.utc))
    assert "&lt;b&gt;table&lt;/b&gt;" in page and "<b>table</b>" not in page
    assert "&lt;script&gt;" in page and "<script>" not in page
    assert "as of <strong>unknown</strong>" in page


def test_the_index_is_not_written_before_the_docs_and_the_picture_exist(site, tmp_path, monkeypatch, capsys):
    def no_warehouse():
        raise AssertionError("connected to the warehouse")

    monkeypatch.setattr(site.gold_fingerprint, "connection", no_warehouse)
    (tmp_path / "dbt").mkdir()
    (tmp_path / "dbt" / "index.html").write_text("docs")
    assert site.main([str(tmp_path)]) == 1
    assert "lacks dashboard.png (from local/site/capture_dashboard.py)" in capsys.readouterr().err
    assert not (tmp_path / "index.html").exists()


def test_the_browser_is_pinned_by_one_playwright_version_and_kept_in_the_local_folder():
    version = re.search(r'^PLAYWRIGHT_VERSION="([^"]+)"', SCRIPT, re.MULTILINE).group(1)
    assert re.fullmatch(r"\d+\.\d+\.\d+", version)
    body = SCRIPT[SCRIPT.index("site_build() {"):SCRIPT.index("\n}\n", SCRIPT.index("site_build() {"))]
    assert '--with "playwright==$PLAYWRIGHT_VERSION"' in body
    assert 'PLAYWRIGHT_BROWSERS_PATH="$REPO/.local/playwright"' in body  
    assert "SUPERSET_PASSWORD" not in body 


def site_build() -> str:
    return SCRIPT[SCRIPT.index("site_build() {"):SCRIPT.index("\n}\n", SCRIPT.index("site_build() {"))]


def test_the_site_can_be_published_as_it_is_built():
    body = site_build()
    assert body.index("build_site.py") < body.index('chmod -R a+rX "$SITE_DIR"')
    assert 'CRM_EXPOSURE_URL="../dashboard.png" dbt_docs' in body
    exposure = (REPO / "dbt" / "models" / "gold" / "reporting" / "_exposures.yml").read_text()
    assert ("env_var('CRM_EXPOSURE_URL', env_var('SUPERSET_URL', 'http://localhost:8088') ~ "
            "'/superset/dashboard/executive-report/')") in exposure
    docs = SCRIPT[SCRIPT.index("dbt_docs() {"):SCRIPT.index("site_build() {")]
    assert "CRM_EXPOSURE_URL" not in docs                       # `scripts/local.sh docs` alone keeps Superset's address


def test_the_browser_s_system_libraries_are_installed_only_when_asked():
    body = site_build()
    assert "local -a install=(install --only-shell chromium)" in body
    assert 'if [ "${CRM_SITE_BROWSER_DEPS:-}" = true ]; then' in body
    assert "install=(install --with-deps --only-shell chromium)" in body
    assert '-m playwright "${install[@]}"' in body
