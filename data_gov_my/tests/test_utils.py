import pandas as pd
import pytest

from data_gov_my.utils.chart_builders import ChartBuilder
from data_gov_my.utils.variable_structures import *

"""
Bar chart
"""


@pytest.fixture
def sample_barchart_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": ["California", "California", "New York", "New York"],
        "period": ["2020", "2021", "2020", "2021"],
        "age_group": ["18-24", "25-34", "18-24", "25-34"],
        "new_donors": [100, 200, 150, 250],
        "old_donors": [1, 2, 3, 4],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_bar_chart(sample_barchart_data):
    variables = {"keys": ["state", "period"], "x": "age_group", "y": ["new_donors"]}

    expected_result = {
        "California": {
            "2020": {"x": ["18-24"], "y": [100]},
            "2021": {"x": ["25-34"], "y": [200]},
        },
        "New York": {
            "2020": {"x": ["18-24"], "y": [150]},
            "2021": {"x": ["25-34"], "y": [250]},
        },
    }

    builder = ChartBuilder.create("bar_chart")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


"""
Bar meter
"""


def test_barmeter_simple(sample_barchart_data):
    """
    Returns bar chart data in [{x:0,y:0},{...}] format
    """
    variables = {
        "keys": ["state"],
        "axis_values": [{"age_group": "new_donors"}],
    }

    expected_result = {
        "California": [{"x": "18-24", "y": 100}, {"x": "25-34", "y": 200}],
        "New York": [{"x": "18-24", "y": 150}, {"x": "25-34", "y": 250}],
    }
    builder = ChartBuilder.create("bar_meter")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


def test_barmeter_subkeys(sample_barchart_data):
    """
    Returns bar chart data in {"sub_key": [{x:0,y:0},{...}] } format
    """
    variables = {
        "keys": ["state", "period"],
        "axis_values": [{"age_group": "new_donors"}, {"age_group": "old_donors"}],
        "sub_keys": True,
    }

    expected_result = {
        "California": {
            "2020": {
                "new_donors": [{"x": "18-24", "y": 100}],
                "old_donors": [{"x": "18-24", "y": 1}],
            },
            "2021": {
                "new_donors": [{"x": "25-34", "y": 200}],
                "old_donors": [{"x": "25-34", "y": 2}],
            },
        },
        "New York": {
            "2020": {
                "new_donors": [{"x": "18-24", "y": 150}],
                "old_donors": [{"x": "18-24", "y": 3}],
            },
            "2021": {
                "new_donors": [{"x": "25-34", "y": 250}],
                "old_donors": [{"x": "25-34", "y": 4}],
            },
        },
    }

    builder = ChartBuilder.create("bar_meter")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


"""
Timeseries
"""


@pytest.fixture
def sample_timeseries_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": [
            "California",
            "California",
            "California",
            "New York",
            "New York",
            "New York",
        ],
        "date": [
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
            "2020-01-01",
            "2020-01-02",
            "2020-01-03",
        ],
        "daily": [100, 200, 150, 120, 180, 140],
        "daily_7dma": [90, 190, 160, 130, 170, 150],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_timeseries_chart(sample_timeseries_data):
    variables = {
        "keys": ["state"],
        "values": {"x": "date", "daily": "daily", "line_daily": "daily_7dma"},
    }

    expected_result = {
        "California": {
            "x": [1577836800000, 1577923200000, 1578009600000],
            "daily": [100, 200, 150],
            "line_daily": [90, 190, 160],
        },
        "New York": {
            "x": [1577836800000, 1577923200000, 1578009600000],
            "daily": [120, 180, 140],
            "line_daily": [130, 170, 150],
        },
    }

    builder = ChartBuilder.create("timeseries_chart")
    result = builder.build_chart(sample_timeseries_data, variables)
    assert result == expected_result


def test_timeseries_shared_chart_simple(sample_timeseries_data):
    variables = {
        "keys": [],
        "constant": {"x": "date"},
        "attributes": {"daily": "daily", "line_daily": "daily_7dma"},
    }
    expected_result = {
        "x": [1577836800000, 1577923200000, 1578009600000],
        "daily": [100, 200, 150, 120, 180, 140],
        "line_daily": [90, 190, 160, 130, 170, 150],
    }

    builder = ChartBuilder.create("timeseries_shared")
    result = builder.build_chart(sample_timeseries_data, variables)
    assert result == expected_result


def test_timeseries_shared_chart_nested(sample_timeseries_data):
    variables = {
        "keys": ["state"],
        "constant": {"x": "date"},
        "attributes": {"daily": "daily", "line_daily": "daily_7dma"},
    }

    expected_result = {
        "x": [1577836800000, 1577923200000, 1578009600000],
        "California": {"daily": [100, 200, 150], "line_daily": [90, 190, 160]},
        "New York": {"daily": [120, 180, 140], "line_daily": [130, 170, 150]},
    }
    builder = ChartBuilder.create("timeseries_shared")
    result = builder.build_chart(sample_timeseries_data, variables)
    assert result == expected_result


"""
Line Chart
"""


@pytest.fixture
def sample_line_data(tmp_path):
    file_name = tmp_path / "test_file.parquet"
    data = {
        "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "state": [
            "California",
            "California",
            "California",
            "California",
            "New York",
            "New York",
            "New York",
            "New York",
            "Texas",
            "Texas",
            "Texas",
            "Texas",
        ],
        "period": [
            "daily",
            "daily",
            "monthly",
            "monthly",
            "daily",
            "daily",
            "monthly",
            "monthly",
            "daily",
            "daily",
            "monthly",
            "monthly",
        ],
        "y1": [100, 200, 150, 170, 120, 180, 140, 160, 130, 110, 135, 155],
        "y2": [50, 80, 120, 140, 90, 160, 110, 130, 100, 70, 90, 110],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_line_chart_simple(sample_line_data):
    variables = {"rename_cols": {"y1": "y"}, "x": "x", "y": ["y"]}

    expected_result = {
        "x": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
        "y": [100, 200, 150, 170, 120, 180, 140, 160, 130, 110, 135, 155],
    }
    builder = ChartBuilder.create("line_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


def test_line_chart_nested_single_layer(sample_line_data):
    """
    TODO: updated test to fit after refactoring (assuming that variables structure will change)
    """
    variables = {"rename_cols": {"y1": "y"}, "keys": ["state"], "x": "x", "y": ["y"]}

    expected_result = {
        "California": {"x": [1, 2, 3, 4], "y": [100, 200, 150, 170]},
        "New York": {"x": [5, 6, 7, 8], "y": [120, 180, 140, 160]},
        "Texas": {"x": [9, 10, 11, 12], "y": [130, 110, 135, 155]},
    }

    builder = ChartBuilder.create("line_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


def test_line_chart_nested_multi_layer(sample_line_data):
    """
    TODO: updated test to fit after refactoring (assuming that variables structure will change)
    """
    variables = {
        "rename_cols": {"y1": "y"},
        "keys": ["state", "period"],
        "x": "x",
        "y": ["y"],
    }

    expected_result = {
        "California": {
            "daily": {"x": [1, 2], "y": [100, 200]},
            "monthly": {"x": [3, 4], "y": [150, 170]},
        },
        "New York": {
            "daily": {"x": [5, 6], "y": [120, 180]},
            "monthly": {"x": [7, 8], "y": [140, 160]},
        },
        "Texas": {
            "daily": {"x": [9, 10], "y": [130, 110]},
            "monthly": {"x": [11, 12], "y": [135, 155]},
        },
    }

    builder = ChartBuilder.create("line_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


"""
Custom Chart
"""


def test_custom_chart(sample_barchart_data):
    variables = {
        "keys": ["state", "period"],
        "value_columns": ["age_group", "new_donors", "old_donors"],
    }

    expected_result = {
        "California": {
            "2020": {"age_group": "18-24", "new_donors": 100, "old_donors": 1},
            "2021": {"age_group": "25-34", "new_donors": 200, "old_donors": 2},
        },
        "New York": {
            "2020": {"age_group": "18-24", "new_donors": 150, "old_donors": 3},
            "2021": {"age_group": "25-34", "new_donors": 250, "old_donors": 4},
        },
    }

    builder = ChartBuilder.create("custom_chart")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


"""
Waffle Chart
"""


@pytest.fixture
def sample_waffle_data(tmp_path):
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": [
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
            "Malaysia",
        ],
        "age_group": [
            "child",
            "child",
            "child",
            "child",
            "child",
            "child",
            "adult",
            "adult",
            "adult",
            "adult",
            "adult",
            "adult",
            "elderly",
            "elderly",
            "elderly",
            "elderly",
            "elderly",
            "elderly",
        ],
        "dose": [
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
            "dose1",
            "dose2",
        ],
        "metric": [
            "total",
            "total",
            "daily",
            "daily",
            "perc",
            "perc",
            "total",
            "total",
            "daily",
            "daily",
            "perc",
            "perc",
            "total",
            "total",
            "daily",
            "daily",
            "perc",
            "perc",
        ],
        "value": [34, 3, 88, 27, 6, 45, 13, 100, 2, 99, 99, 70, 62, 61, 87, 90, 10, 63],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_waffle_chart(sample_waffle_data):
    variables = {
        "wanted": [],
        "keys": ["state", "age_group", "dose"],
        "dict_keys": ["metric", "value"],
        "data_arr": {"id": "dose", "label": "dose", "value": {"metric": "perc"}},
    }

    expected_result = {
        "mys": {
            "child": {
                "dose1": {
                    "total": 34,
                    "daily": 88,
                    "perc": 6,
                    "data": [{"id": "dose1", "label": "dose1", "value": 6}],
                },
                "dose2": {
                    "total": 3,
                    "daily": 27,
                    "perc": 45,
                    "data": [{"id": "dose2", "label": "dose2", "value": 45}],
                },
            },
            "adult": {
                "dose1": {
                    "total": 13,
                    "daily": 2,
                    "perc": 99,
                    "data": [{"id": "dose1", "label": "dose1", "value": 99}],
                },
                "dose2": {
                    "total": 100,
                    "daily": 99,
                    "perc": 70,
                    "data": [{"id": "dose2", "label": "dose2", "value": 70}],
                },
            },
            "elderly": {
                "dose1": {
                    "total": 62,
                    "daily": 87,
                    "perc": 10,
                    "data": [{"id": "dose1", "label": "dose1", "value": 10}],
                },
                "dose2": {
                    "total": 61,
                    "daily": 90,
                    "perc": 63,
                    "data": [{"id": "dose2", "label": "dose2", "value": 63}],
                },
            },
        }
    }

    builder = ChartBuilder.create("waffle_chart")
    result = builder.build_chart(sample_waffle_data, variables)
    assert result == expected_result


"""
Choropleth
"""


@pytest.fixture
def sample_choropleth_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": [
            "Johor",
            "Kedah",
            "Kelantan",
            "Melaka",
            "Negeri Sembilan",
            "Pahang",
            "Perak",
            "Pulau Pinang",
            "Sabah",
            "Sarawak",
            "Selangor",
            "Terengganu",
            "W.P. Kuala Lumpur",
            "Perlis",
            "W.P. Labuan",
            "W.P. Putrajaya",
        ],
        "y1": [
            0.43,
            0.39,
            0.27,
            0.73,
            0.7,
            0.85,
            0.69,
            0.05,
            0.62,
            0.08,
            0.83,
            0.89,
            0.12,
            0.91,
            0.72,
            0.49,
        ],
        "y2": [
            334,
            548,
            852,
            300,
            497,
            799,
            493,
            495,
            633,
            471,
            497,
            575,
            536,
            778,
            506,
            520,
        ],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_choropleth_chart(sample_choropleth_data):
    variables = {"x": "state", "y": ["y1", "y2"]}

    expected_result = {
        "x": [
            "jhr",
            "kdh",
            "ktn",
            "mlk",
            "nsn",
            "phg",
            "prk",
            "png",
            "sbh",
            "swk",
            "sgr",
            "trg",
            "kul",
            "pls",
            "lbn",
            "pjy",
        ],
        "y": {
            "y1": [
                0.43,
                0.39,
                0.27,
                0.73,
                0.7,
                0.85,
                0.69,
                0.05,
                0.62,
                0.08,
                0.83,
                0.89,
                0.12,
                0.91,
                0.72,
                0.49,
            ],
            "y2": [
                334,
                548,
                852,
                300,
                497,
                799,
                493,
                495,
                633,
                471,
                497,
                575,
                536,
                778,
                506,
                520,
            ],
        },
    }

    builder = ChartBuilder.create("choropleth_chart")
    result = builder.build_chart(sample_choropleth_data, variables)
    assert result == expected_result


"""
Metrics Table
"""


@pytest.fixture
def sample_metrics_table_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "lang": ["bm", "bm", "bm", "en", "en", "en"],
        "fruit": ["nanas", "epal", "tembikai", "pineapple", "apple", "watermelon"],
    }

    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_metrics_table(sample_metrics_table_data):
    variables = {"keys": ["lang"], "value_columns": ["fruit"]}
    expected_result = {
        "bm": [{"fruit": "nanas"}, {"fruit": "epal"}, {"fruit": "tembikai"}],
        "en": [{"fruit": "pineapple"}, {"fruit": "apple"}, {"fruit": "watermelon"}],
    }
    builder = ChartBuilder.create("metrics_table")
    result = builder.build_chart(sample_metrics_table_data, variables)
    assert result == expected_result


"""
Query values (Dropdown)
"""


def test_query_values_simple(sample_barchart_data):
    variables = {"columns": ["state"]}
    expected_result = ["California", "New York"]

    builder = ChartBuilder.create("query_values")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


def test_query_values_flat(sample_barchart_data):
    variables = {"columns": ["state", "period"], "flat": True}
    expected_result = [
        {"state": "California", "period": "2020"},
        {"state": "California", "period": "2021"},
        {"state": "New York", "period": "2020"},
        {"state": "New York", "period": "2021"},
    ]
    builder = ChartBuilder.create("query_values")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


def test_query_values_nested_single_layer(sample_barchart_data):
    variables = {"columns": ["state", "period"], "flat": False}
    expected_result = {"California": ["2020", "2021"], "New York": ["2020", "2021"]}
    builder = ChartBuilder.create("query_values")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


def test_query_values_nested_multi_layer(sample_barchart_data):
    variables = {"columns": ["state", "period", "age_group"], "flat": False}
    expected_result = {
        "California": {"2020": ["18-24"], "2021": ["25-34"]},
        "New York": {"2020": ["18-24"], "2021": ["25-34"]},
    }
    builder = ChartBuilder.create("query_values")
    result = builder.build_chart(sample_barchart_data, variables)
    assert result == expected_result


"""
Heatmap
"""


def test_heatmap_chart_simple(sample_line_data):
    variables = {
        "rename_cols": {"y1": "Y1"},
        "value_columns": ["Y1"],
        "keys": ["state"],
    }
    expected_result = {
        "California": [
            {"x": "Y1", "y": 100},
            {"x": "Y1", "y": 200},
            {"x": "Y1", "y": 150},
            {"x": "Y1", "y": 170},
        ],
        "New York": [
            {"x": "Y1", "y": 120},
            {"x": "Y1", "y": 180},
            {"x": "Y1", "y": 140},
            {"x": "Y1", "y": 160},
        ],
        "Texas": [
            {"x": "Y1", "y": 130},
            {"x": "Y1", "y": 110},
            {"x": "Y1", "y": 135},
            {"x": "Y1", "y": 155},
        ],
    }
    builder = ChartBuilder.create("heatmap_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


def test_heatmap_chart_multi_cols(sample_line_data):
    variables = {
        "rename_cols": {"y1": "Y1", "y2": "Y2"},
        "value_columns": ["Y1", "Y2"],
        "keys": ["period"],
    }
    expected_result = {
        "daily": [
            {"x": "Y1", "y": 100},
            {"x": "Y2", "y": 50},
            {"x": "Y1", "y": 200},
            {"x": "Y2", "y": 80},
            {"x": "Y1", "y": 120},
            {"x": "Y2", "y": 90},
            {"x": "Y1", "y": 180},
            {"x": "Y2", "y": 160},
            {"x": "Y1", "y": 130},
            {"x": "Y2", "y": 100},
            {"x": "Y1", "y": 110},
            {"x": "Y2", "y": 70},
        ],
        "monthly": [
            {"x": "Y1", "y": 150},
            {"x": "Y2", "y": 120},
            {"x": "Y1", "y": 170},
            {"x": "Y2", "y": 140},
            {"x": "Y1", "y": 140},
            {"x": "Y2", "y": 110},
            {"x": "Y1", "y": 160},
            {"x": "Y2", "y": 130},
            {"x": "Y1", "y": 135},
            {"x": "Y2", "y": 90},
            {"x": "Y1", "y": 155},
            {"x": "Y2", "y": 110},
        ],
    }
    builder = ChartBuilder.create("heatmap_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


"""
Snapshot chart
"""


def test_snapshot_chart(sample_line_data):
    """ """
    variables = {
        "main_key": "state",
        "replace_word": "",
        "null_vals": None,
        "data": {
            "y": ["y1", "y2"],
        },
    }
    expected_result = [
        {"y": {"y1": 100, "y2": 50}, "index": 0, "state": "California"},
        {"y": {"y1": 200, "y2": 80}, "index": 1, "state": "California"},
        {"y": {"y1": 150, "y2": 120}, "index": 2, "state": "California"},
        {"y": {"y1": 170, "y2": 140}, "index": 3, "state": "California"},
        {"y": {"y1": 120, "y2": 90}, "index": 4, "state": "New York"},
        {"y": {"y1": 180, "y2": 160}, "index": 5, "state": "New York"},
        {"y": {"y1": 140, "y2": 110}, "index": 6, "state": "New York"},
        {"y": {"y1": 160, "y2": 130}, "index": 7, "state": "New York"},
        {"y": {"y1": 130, "y2": 100}, "index": 8, "state": "Texas"},
        {"y": {"y1": 110, "y2": 70}, "index": 9, "state": "Texas"},
        {"y": {"y1": 135, "y2": 90}, "index": 10, "state": "Texas"},
        {"y": {"y1": 155, "y2": 110}, "index": 11, "state": "Texas"},
    ]

    builder = ChartBuilder.create("snapshot_chart")
    result = builder.build_chart(sample_line_data, variables)
    assert result == expected_result


"""
Map Lat Long
"""


@pytest.fixture
def sample_map_lat_long_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": ["California", "California", "New York", "New York", "Texas"],
        "district": [
            "Los Angeles",
            "San Francisco",
            "New York City",
            "Buffalo",
            "Houston",
        ],
        "lat": [34.0522, 37.7749, 40.7128, 42.8864, 29.7604],  # Latitude values
        "long": [-118.2437, -122.4194, -74.0060, -78.8784, -95.3698],  # Longitude value
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_map_lat_long_chart(sample_map_lat_long_data):
    variables = {
        "keys": ["state", "district"],
        "value_columns": ["lat", "long"],  # Updated column names
        "null_vals": -1,
    }
    expected_result = {
        "California": {
            "los-angeles": [{"lat": 34.0522, "long": -118.2437}],
            "san-francisco": [{"lat": 37.7749, "long": -122.4194}],
        },
        "New York": {
            "new-york-city": [{"lat": 40.7128, "long": -74.006}],
            "buffalo": [{"lat": 42.8864, "long": -78.8784}],
        },
        "Texas": {"houston": [{"lat": 29.7604, "long": -95.3698}]},
    }
    builder = ChartBuilder.create("map_lat_lon")
    result = builder.build_chart(sample_map_lat_long_data, variables)
    assert result == expected_result
