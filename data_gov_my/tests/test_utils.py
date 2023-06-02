import pytest

from data_gov_my.utils.chart_builder import *
from data_gov_my.utils.variable_structures import *


@pytest.fixture
def sample_barchart_data(tmp_path):
    # Create a temporary test file with sample data
    file_name = tmp_path / "test_file.parquet"
    data = {
        "state": ["California", "California", "New York", "New York"],
        "period": ["2020", "2021", "2020", "2021"],
        "age_group": ["18-24", "25-34", "18-24", "25-34"],
        "new_donors": [100, 200, 150, 250],
    }
    df = pd.DataFrame(data)
    df.to_parquet(file_name)
    return str(file_name)


def test_bar_chart(sample_barchart_data):
    variables = {
        "keys": ["state", "period"],
        "axis_values": ["age_group", "new_donors"],
    }

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

    result = bar_chart(sample_barchart_data, variables)
    assert result == expected_result


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

    result = timeseries_chart(sample_timeseries_data, variables)
    assert result == expected_result
