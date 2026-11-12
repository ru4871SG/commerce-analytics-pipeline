"""The dashboard and dbt describe the same tables, and must agree."""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[1]
REPORTING = REPO / "dbt" / "models" / "gold" / "reporting"

spec = importlib.util.spec_from_file_location("build_bundle", REPO / "local" / "superset" / "build_bundle.py")
build_bundle = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build_bundle)
DATASETS, CHARTS = build_bundle.DATASETS, build_bundle.CHARTS

spec = importlib.util.spec_from_file_location("check_dashboard", REPO / "local" / "superset" / "check_dashboard.py")
check_dashboard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_dashboard)

MODELS = {model["name"]: model for model in yaml.safe_load((REPORTING / "_reporting.yml").read_text())["models"]}
EXPOSURE = yaml.safe_load((REPORTING / "_exposures.yml").read_text())["exposures"][0]


def contract(model: str) -> dict[str, str]:
    """The model's contract: column name -> data type."""
    return {column["name"]: column.get("data_type") for column in MODELS[model]["columns"]}


def dataset_of(chart: dict) -> str:
    return next(name for name, definition in DATASETS.items() if definition["uuid"] == chart["dataset_uuid"])



def test_the_exposure_depends_on_exactly_the_models_the_dashboard_reads():
    assert (EXPOSURE["name"], EXPOSURE["type"]) == ("executive_report", "dashboard")
    models = re.findall(r"ref\('([a-z_]+)'\)", " ".join(EXPOSURE["depends_on"]))
    assert len(models) == len(EXPOSURE["depends_on"])           # nothing but refs, each one read
    assert sorted(models) == sorted(DATASETS)


def test_the_exposure_names_its_owner_without_an_email_address():
    assert EXPOSURE["owner"] == {"name": "Ruddy Gunawan"}
    assert "@" not in (REPORTING / "_exposures.yml").read_text()


def test_contracts_are_enforced_for_the_whole_reporting_folder():
    project = yaml.safe_load((REPO / "dbt" / "dbt_project.yml").read_text())
    assert project["models"]["executive_report"]["gold"]["reporting"]["+contract"] == {"enforced": True}


def test_every_reporting_model_is_described_and_the_dashboard_reads_each_of_them():
    files = sorted(path.stem for path in REPORTING.glob("*.sql"))
    assert files == sorted(MODELS)
    assert files == sorted(DATASETS)


@pytest.mark.parametrize("model", sorted(MODELS))
def test_every_column_of_a_contract_has_a_type(model):
    untyped = [name for name, data_type in contract(model).items() if not data_type]
    assert untyped == []


@pytest.mark.parametrize("model", sorted(MODELS))
def test_every_reporting_model_carries_the_brand_and_the_name_the_filter_uses(model):
    assert {"brand": "varchar", "brand_name": "varchar"}.items() <= contract(model).items()


CAST = re.compile(r"::([a-z_]+(?:\(\d+, \d+\))?) as ([a-z0-9_]+(?:\{\{ [a-z_]+ \}\}[a-z0-9_]*)*)")
JINJA_VARIABLE = re.compile(r"\{\{ [a-z_]+ \}\}")


def casts(model: str) -> list[tuple[re.Pattern, str]]:
    """The final select's columns as (name pattern, the type it is cast to). A name written with
    a Jinja variable, revenue_{{ name }}_usd, stands for every column its loop writes."""
    sql = (REPORTING / f"{model}.sql").read_text()
    final = sql[sql.index("-- Each column is cast to its contract type"):]
    final = " ".join(final[:final.index("\nfrom ")].split())
    return [(re.compile("[a-z0-9]+".join(re.escape(part) for part in JINJA_VARIABLE.split(name))), data_type)
            for data_type, name in CAST.findall(final)]


@pytest.mark.parametrize("model", sorted(MODELS))
def test_the_final_select_casts_every_column_to_the_type_its_contract_declares(model):
    # dbt compares type families only: a column declared int whose SQL yields number(18, 2)
    # passes the build, and the table then rounds it. This is the check on precision and scale.
    found = casts(model)
    for column, declared in contract(model).items():
        cast_to = {data_type for pattern, data_type in found if pattern.fullmatch(column)}
        assert cast_to == {declared}, f"{model}.{column}"


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_every_column_a_dataset_declares_is_in_the_models_contract(dataset):
    declared = [column["column_name"] for column in DATASETS[dataset]["columns"]]
    assert [name for name in declared if name not in contract(dataset)] == []


def family(data_type: str) -> str:
    """The Superset type a contract type shows as."""
    if data_type == "varchar":
        return "VARCHAR"
    if data_type == "date":
        return "TIMESTAMP"
    if data_type == "int":
        return "BIGINT"
    if data_type == "boolean":
        return "BOOLEAN"
    scale = re.fullmatch(r"number\(\d+, (\d+)\)", data_type)
    if data_type == "float" or (scale and int(scale.group(1)) > 0):
        return "DOUBLE"
    raise AssertionError(f"{data_type} is not a type the reporting contracts use")


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_the_type_superset_is_told_agrees_with_the_contract(dataset):
    types = contract(dataset)
    wrong = {column["column_name"]: (column["type"], types[column["column_name"]])
             for column in DATASETS[dataset]["columns"] if family(types[column["column_name"]]) != column["type"]}
    assert wrong == {}


@pytest.mark.parametrize("dataset", sorted(DATASETS))
def test_every_metric_reads_columns_its_dataset_declares(dataset):
    declared = {column["column_name"] for column in DATASETS[dataset]["columns"]}
    for metric in DATASETS[dataset]["metrics"]:
        # Functions are written in capitals, columns in lower case
        columns = set(re.findall(r"\b[a-z_][a-z0-9_]*\b", metric["expression"]))
        assert columns and columns <= declared, metric["metric_name"]


@pytest.mark.parametrize("chart", CHARTS, ids=lambda chart: chart["slice_name"])
def test_every_chart_reads_columns_and_metrics_its_dataset_declares(chart):
    dataset = DATASETS[dataset_of(chart)]
    columns = {column["column_name"] for column in dataset["columns"]}
    metrics = {metric["metric_name"] for metric in dataset["metrics"]}
    params = chart["params"]
    read = [params.get("x_axis"), *params.get("groupby", []), *params.get("groupby_b", []),
            *params.get("groupbyRows", []), *params.get("groupbyColumns", []), *params.get("all_columns", []),
            *(json.loads(item)[0] for item in params.get("order_by_cols", []))]
    # A filter written in SQL: its lower-case words are columns (true, false and the like aside)
    for item in params.get("adhoc_filters", []):
        read += [word for word in re.findall(r"\b[a-z_][a-z0-9_]*\b", item["sqlExpression"])
                 if word not in ("true", "false", "null", "and", "or", "not", "is")]
    assert [name for name in read if name and name not in columns] == []
    used = [params.get("metric"), *params.get("metrics", []), *params.get("metrics_b", [])]
    assert [name for name in used if name and name not in metrics] == []
    # A chart with no metric draws nothing, unless it is a table of the rows as they are stored
    assert any(used) or params.get("all_columns")


def test_every_mixed_chart_names_series_types_superset_draws():
    """A series type Superset does not know is not an error: the bars come out as lines. The
    name of a chart plugin (echarts_timeseries_bar) is such a value."""
    mixed = [chart for chart in CHARTS if chart["viz_type"] == "mixed_timeseries"]
    assert len(mixed) >= 2
    for chart in mixed:
        for key in ("seriesType", "seriesTypeB"):
            assert chart["params"].get(key) in build_bundle.MIXED_SERIES_TYPES, (chart["slice_name"], key)
    # Both are described as bars with a line over them
    assert {(chart["params"]["seriesType"], chart["params"]["seriesTypeB"]) for chart in mixed} == {("bar", "line")}



def test_the_dashboard_check_asks_for_time_wherever_a_chart_has_a_time_axis():
    # check_dashboard.py names the columns it queries as a time axis. A chart over a time column
    # it does not know would be queried as if the column were a category, not as the chart is.
    time_columns = {name: {column["column_name"] for column in definition["columns"] if column["is_dttm"]}
                    for name, definition in DATASETS.items()}
    time_charts = [chart for chart in CHARTS if chart["params"].get("x_axis") in time_columns[dataset_of(chart)]]
    assert {chart["params"]["x_axis"] for chart in time_charts} == {"date_day", "week_start", "month_start"}
    for chart in time_charts:
        query = check_dashboard.query_for(chart)
        assert (query["granularity"], query["is_timeseries"]) == (chart["params"]["x_axis"], True), chart["slice_name"]


def test_the_dashboard_check_queries_a_chart_with_the_charts_own_filter():
    # A chart that shows a part of its table (the as-of day, the months that are over) says so in
    # a SQL filter. Queried without it, the check would pass on rows the chart does not show, and
    # revenue churn would be read from a month that is not over.
    filtered = {chart["slice_name"]: check_dashboard.query_for(chart)["extras"]
                for chart in CHARTS if chart["params"].get("adhoc_filters")}
    assert filtered == {
        "Unusual numbers on the as-of day": {"where": "(is_as_of_day = true)"},
        "Revenue churn, last complete month": {"where": "(is_complete_month = true)"},
    }
