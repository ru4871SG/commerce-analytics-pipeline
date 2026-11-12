#!/usr/bin/env python3
"""Write the index page of the local site.
"""
from __future__ import annotations

import html
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "local"))
import gold_fingerprint  # noqa: E402

BUILT_BEFORE = {"dbt/index.html": "scripts/local.sh docs", "dashboard.png": "local/site/capture_dashboard.py"}

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Commerce Analytics Pipeline</title>
<style>
  :root {{ color-scheme: light dark; --text: #1f2328; --muted: #59636e; --line: #d0d7de; --bg: #ffffff; --link: #0969da; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --text: #e6edf3; --muted: #9198a1; --line: #30363d; --bg: #0d1117; --link: #4493f8; }}
  }}
  body {{ margin: 0 auto; max-width: 1100px; padding: 32px 20px 64px; background: var(--bg); color: var(--text);
         font: 16px/1.55 -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif; }}
  h1 {{ font-size: 30px; margin: 0 0 4px; }}
  h2 {{ font-size: 21px; margin: 44px 0 10px; padding-bottom: 6px; border-bottom: 1px solid var(--line); }}
  p {{ margin: 8px 0; }}
  a {{ color: var(--link); }}
  .muted {{ color: var(--muted); }}
  img {{ max-width: 100%; height: auto; border: 1px solid var(--line); border-radius: 8px; }}
  table {{ border-collapse: collapse; margin-top: 8px; }}
  th, td {{ padding: 5px 16px 5px 0; text-align: left; border-bottom: 1px solid var(--line); }}
  td.number, th.number {{ text-align: right; font-variant-numeric: tabular-nums; }}
  code {{ font: 14px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }}
</style>
</head>
<body>
<h1>Commerce Analytics Pipeline</h1>
<p class="muted">PostgreSQL, Airflow or Dagster, Snowflake and dbt, Superset, with Spark and Databricks for the
clickstream. Everything on this page was built by one run of the pipeline on emulators, with fictional data.</p>
<p>Gold is as of <strong>{as_of}</strong>. This page was built {built_at} from commit <code>{commit}</code>.</p>
{source}

<h2>The executive report</h2>
<p>The Superset dashboard, captured once every chart had drawn. <a href="dashboard.png">Open the picture</a>.</p>
<a href="dashboard.png"><img src="dashboard.png" alt="The Executive Report dashboard in Superset"></a>

<h2>The pipeline</h2>
<img src="pipeline.svg" alt="The end-to-end pipeline, from the CRM databases and the clickstream to the dashboard">

<h2>The dbt project</h2>
<p><a href="dbt/index.html">The dbt docs</a>: every model, its columns and tests, and the lineage from the sources
to the dashboard, which dbt knows as the exposure <code>executive_report</code>.</p>

<h2>The reporting tables</h2>
<p>The <code>exec_*</code> tables the dashboard reads, as of {as_of}. Their numeric sums are in
<a href="gold-fingerprint.json">gold-fingerprint.json</a>.</p>
<table>
<tr><th>Table</th><th class="number">Rows</th></tr>
{rows}
</table>
</body>
</html>
"""


def render(as_of: date | None, fingerprint: dict, commit: str, built_at: datetime,
           repository_url: str | None = None) -> str:
    """The index page. fingerprint is local/gold_fingerprint.py's: table -> {"rows": n, "sums": {...}}.
    repository_url: where the code is, for a page that is published; a link, nothing is fetched."""
    rows = "\n".join(
        f'<tr><td><code>{html.escape(table)}</code></td><td class="number">{values["rows"]:,}</td></tr>'
        for table, values in sorted(fingerprint.items())
    )
    source = ""
    if repository_url:
        address = html.escape(repository_url, quote=True)
        source = (f'<p>The code, and how to run all of this on a laptop or in a browser: '
                  f'<a href="{address}">{address}</a>.</p>')
    return PAGE.format(
        as_of=html.escape(as_of.isoformat() if as_of else "unknown"),
        built_at=html.escape(built_at.strftime("%Y-%m-%d %H:%M UTC")),
        commit=html.escape(commit),
        source=source,
        rows=rows,
    )


def repository_url(environment=None) -> str | None:
    """The repository's address when the page is built by a GitHub workflow, else None."""
    environment = os.environ if environment is None else environment
    repository = environment.get("GITHUB_REPOSITORY")
    if not repository:
        return None
    return f"{environment.get('GITHUB_SERVER_URL', 'https://github.com').rstrip('/')}/{repository}"


def commit_label() -> str:
    """The commit the page was built from, and whether the checkout had changes beyond it."""
    def git(*args: str) -> str:
        return subprocess.run(["git", "-C", str(REPO), *args], capture_output=True, text=True).stdout.strip()

    commit = git("rev-parse", "--short", "HEAD") or "unknown"
    return f"{commit}, with uncommitted changes" if git("status", "--porcelain") else commit


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if len(argv) != 1:
        print("usage: build_site.py <site folder>", file=sys.stderr)
        return 2
    site = Path(argv[0])
    missing = [f"{name} (from {source})" for name, source in BUILT_BEFORE.items() if not (site / name).is_file()]
    if missing:
        print(f"{site} lacks {' and '.join(missing)}: run scripts/local.sh site, which builds them first.",
              file=sys.stderr)
        return 1

    import snowflake.connector

    conn = snowflake.connector.connect(**gold_fingerprint.connection())
    try:
        cursor = conn.cursor()
        fingerprint = gold_fingerprint.fingerprint(cursor)
        cursor.execute("select max(as_of_date) from ANALYTICS.GOLD.EXEC_KPI_SNAPSHOT")
        as_of = cursor.fetchone()[0]
    finally:
        conn.close()

    shutil.copyfile(REPO / "docs" / "pipeline.svg", site / "pipeline.svg")
    (site / "gold-fingerprint.json").write_text(json.dumps(fingerprint, indent=2, sort_keys=True) + "\n")
    (site / "index.html").write_text(
        render(as_of, fingerprint, commit_label(), datetime.now(timezone.utc), repository_url()))
    print(f"Site in {site}: index.html, dbt/index.html, dashboard.png, pipeline.svg, gold-fingerprint.json "
          f"({len(fingerprint)} exec_* tables, as of {as_of}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
