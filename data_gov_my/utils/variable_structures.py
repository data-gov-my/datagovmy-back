from typing import Dict, List, TypedDict, Any
from pydantic import BaseModel, validator, ValidationError

## TODO: GeneralChartVariable - common fields for all variables, e.g. keys, null_vals


class GeneralChartVariables(BaseModel):
    """
    Commonly shared properties for all charts
    keys: used as keys in the resulted nested dictionary data
    values: columns that contain the actual values of chart data, e.g. y-values
    rename: dict
    """

    keys: list[str]
    value_columns: list[str] = None
    rename_cols: dict[str, str] = {}
    null_vals: str | int | None = None


class TimeseriesChartVariables(TypedDict):
    keys: List[str]
    values: List[str]


class BarChartVariables(GeneralChartVariables):
    x: str
    y: list[str] | dict[str, str]
    # @validator("axis_values")
    # def axis_values_list_length(cls, v):
    #     """
    #     If axis values is in list form, it must be of length two for x-axis and y-axis respectively.
    #     """
    #     if isinstance(v, list) and len(v) != 2:
    #         raise ValueError(f"Length of axis values list must be 2 (not {len(v)})")
    #     return v


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


class SnapshotChartVariables(TypedDict):
    main_key: str
    replace_word: str
    null_vals: str | int | None
    data: Dict[str, Dict[str, List[str]]]


class CustomChartVariables(GeneralChartVariables):
    pass


class ChoroplethChartVariables(TypedDict):
    x: str
    y: List[str]  # alow multiple y columns
    keys: List[str]  # filters


class MetricsTableVariables(TypedDict):
    keys: List[str]
    obj_attr: Dict[str, str]


class PyramidChartVariables(TypedDict):
    col_range: Dict[str, str]
    suffix: Dict[str, str]
    keys: str


class JitterChartVariables(TypedDict):
    keys: str
    id: str
    columns: Dict[str, List[str]]
    tooltip: bool


class HeatmapChartVariables(TypedDict):
    cols: List[str]
    id: str
    keys: List[str]
    null_values: str | int | None
    replace_vals: Dict[str, str | int]
    dict_rename: Dict[str, str]
    row_format: str
    operation: str


class WaffleChartVariables(TypedDict):
    wanted: List[str]
    groups: List[str]
    dict_keys: List[str]
    data_arr: Dict[str, Dict | str]


class LatLonVariables(TypedDict):
    keys: List[str]
    values: List[str]
    null_vals: str | int | None


class SortValuesVariables(TypedDict):
    by: List[str]
    ascending: List[bool]


class QueryValuesVariables(TypedDict):
    flat: bool
    columns: List[str]
    sort_values: SortValuesVariables
