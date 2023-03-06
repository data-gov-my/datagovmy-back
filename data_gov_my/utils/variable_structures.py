from typing import List, Dict, TypedDict


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


class CustomChartVariables(TypedDict):
    keys: List[str]
    columns: List[str]
    null_vals: str | int | None


class ChoroplethChartVariables(TypedDict):
    cols_list: List[str]
    area_key: str


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
