from typing import Dict, List, TypedDict
from pydantic import BaseModel, validator, model_validator


class GeneralChartVariables(BaseModel):
    """
    Commonly shared properties for all charts
    keys: used as keys in the resulted nested dictionary data
    value_columns: columns that contain the actual values of chart data, e.g. y-values
    rename_cols: Dictionary used to rename column names, this should be reflected during pre-processing phase.
    null_vals: Value used to replace nan values, this should be reflected during pre-processing phase.
    replace_vals: Dictionary used to replace df values, this should be reflected during pre-processing phase.
    filter: Dictionary used to filter valid values by column, this should be reflected during pre-processing phase.
    """

    keys: list[str] = []
    value_columns: list[str] = []
    rename_cols: dict[str, str] = {}
    null_vals: str | int | None = None
    replace_vals: dict[str, str | int] = {}
    filter: dict[str, list[str]] = {}


class TimeseriesChartVariables(GeneralChartVariables):
    constants: list[str] = []


class BarChartVariables(GeneralChartVariables):
    x: str
    y: list[str]


class LineChartVariables(GeneralChartVariables):
    x: str
    y: list[str]


class BarMeterVariables(GeneralChartVariables):
    axis_values: list[dict[str, str]]
    sub_keys: bool = False

    @validator("axis_values")
    def axis_values_pair_length(cls, v):
        for pair in v:
            if len(pair) != 1:
                raise ValueError(
                    f"Each dictionary in axis_values should only 1 key (not {len(pair)})"
                )
        return v


class SnapshotChartVariables(GeneralChartVariables):
    main_key: str
    replace_word: str
    data: Dict[str, list[str]]


class CustomChartVariables(GeneralChartVariables):
    pass


class ChoroplethChartVariables(GeneralChartVariables):
    x: str
    y: list[str]  # alow multiple y columns


class MetricsTableVariables(GeneralChartVariables):
    pass


class PyramidChartVariables(GeneralChartVariables):
    label_column: str = "age"
    y1: str = "female"
    y2: str = "male"


class JitterChartVariables(GeneralChartVariables):
    id: str
    columns: dict[str, list[str]]
    tooltip: bool


class HeatmapChartVariables(GeneralChartVariables):
    x: str
    y: str
    z: str


class WaffleChartVariables(GeneralChartVariables):
    dict_keys: list[str]
    data_arr: dict[str, dict | str]

    @validator("dict_keys")
    def dict_keys_length(cls, v):
        if len(v) != 2:
            raise ValueError(
                f"dict_keys must have exactly 2 elements, key col & val col (not {len(v)})"
            )
        return v


class LatLonVariables(TypedDict):
    keys: List[str]
    values: List[str]
    null_vals: str | int | None


class _SortValues(BaseModel):
    by: list[str] = []
    ascending: list[bool] = []

    @model_validator(mode="after")
    def sort_values_length(cls, m: "_SortValues"):
        by, asc = m.by, m.ascending
        if len(by) != len(asc):
            raise ValueError(f"'by' and 'ascending' must be the same length")
        return m


class QueryValuesVariables(GeneralChartVariables):
    flat: bool = False
    columns: list[str] = []  # similar to keys
    sort_values: _SortValues = {}
