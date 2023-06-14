from typing import Dict, List, TypedDict, Any
from pydantic import BaseModel, validator, ValidationError

## TODO: GeneralChartVariable - common fields for all variables, e.g. keys, null_vals

class TimeseriesChartVariables(TypedDict) : 
    keys : List[str]
    values : List[str]

class BarChartVariables(TypedDict):
    keys: List[str]
    axis_values: List[str]


class BarMeterVariables(TypedDict):
    axis_values: List[Dict]
    keys: List[str]
    null_vals: str | int | None
    add_key: Dict[str, str]
    wanted: List[str]
    id_needed: bool
    condition: Dict[str, str | int | None]
    post_operation: str


class SnapshotChartVariables(TypedDict):
    main_key: str
    replace_word: str
    null_vals: str | int | None
    data: Dict[str, Dict[str, List[str]]]


class CustomChartVariables(BaseModel):
    keys: list[str]
    columns: List[str]
    null_vals: str | int | None = None


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
