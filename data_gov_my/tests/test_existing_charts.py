import os
import pytest
import json
from data_gov_my.utils.chart_builders import ChartBuilder
from data_gov_my.utils.dashboard_builder import *
from data_gov_my.utils.variable_structures import *
import pathlib


# get list of chart builders
def get_chart_types(charts=[]):
    if charts:
        return charts
    chart_types = []
    path = os.path.join(os.getcwd(), "data_gov_my", "tests", "chart_expected_output/")
    for dir in os.listdir(path):
        if os.path.isdir(os.path.join(path, dir)):
            chart_types.append(dir)
    return chart_types


@pytest.mark.parametrize("chart_type", get_chart_types(["metrics_table"]))
def test_all_chart_builders(chart_type):
    path = os.path.join(os.getcwd(), "data_gov_my", "tests", "chart_expected_output/")
    files = [f for f in os.listdir(os.path.join(path, chart_type))]
    failed = []

    for file in files:
        with open(os.path.join(path, chart_type, file), "r") as json_file:
            data = json.load(json_file)
            chart_type = data["chart_type"]
            chart_param = data["chart_param"]
            expected_results = data["expected_results"]
            builder = ChartBuilder.create(chart_type)
            results = builder.build_chart(
                chart_param["input"], chart_param["variables"]
            )
            assert expected_results == json.loads(
                json.dumps(results)
            ), f"FAILED: {pathlib.PurePath(file).name}"
