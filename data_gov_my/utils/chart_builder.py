import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from mergedeep import merge

from data_gov_my.utils.general_chart_helpers import *
from data_gov_my.utils.variable_structures import *

"""
Builds Bar Chart
"""


def bar_chart(file_name: str, variables: BarChartVariables):

    df = pd.read_parquet(file_name)

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    if "district" in df.columns:  # District usually uses has spaces and Uppercase
        df["district"] = df["district"].apply(lambda x: x.lower().replace(" ", "-"))

    # Remove later on!!
    if "type" in df.columns:
        df["type"] = df["type"].apply(lambda x: x.lower())

    df = df.replace({np.nan: None})

    keys = variables["keys"]
    axis_values = variables["axis_values"]

    df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    res = {}

    for group in u_groups_list:
        result = {}
        for b in group[::-1]:
            result = {b: result}
        if isinstance(axis_values, list):
            group_l = [group[0]] if len(group) == 1 else list(group)
            group = group[0] if len(group) == 1 else group
            x_list = df.groupby(keys)[axis_values[0]].get_group(group).to_list()
            y_list = df.groupby(keys)[axis_values[1]].get_group(group).to_list()
            final_d = {"x": x_list, "y": y_list}
            set_dict(result, group_l, final_d, "SET")
        else:
            final_d = {}
            for k, v in axis_values.items():
                if k == "X_Y":
                    final_d["x"] = v
                    final_d["y"] = (
                        df.groupby(keys).get_group(group)[v].values.flatten().tolist()
                    )
                else:
                    final_d[k] = df.groupby(keys)[v].get_group(group).to_list()
            set_dict(result, list(group), final_d, "SET")
        merge(res, result)

    return res

"""
Builds Bar Meter
"""

def bar_meter(file_name, variables):
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    if "district" in df.columns:  # District usually uses has spaces and Uppercase
        df["district"] = df["district"].apply(lambda x: x.lower().replace(" ", "-"))

    group_columns = variables["keys"]
    value_columns = variables["axis_values"]
    sub_keys = "sub_keys" in variables # For when each obj in axis_values, should be nested

    def group_to_dict(group):
        result = {} if sub_keys else []
        for d in value_columns:
            for key, value in d.items():
                x_values = group[key].values
                y_values = group[value].values
                if sub_keys : 
                    result[value] = [{"x": x, "y": y} for x, y in zip(x_values, y_values)]
                else : 
                    result.extend([{"x": x, "y": y} for x, y in zip(x_values, y_values)])
        return result       

    if not group_columns:
        return group_to_dict(df)
    
    result = {}
    for name, group in df.groupby(group_columns):
        if len(group_columns) > 1:
            current_level = result
            for i in range(len(name)-1):
                current_level = current_level.setdefault(name[i], {})
            current_level[name[-1]] = group_to_dict(group)
        else:
            result[name] = group_to_dict(group)
    
    return result

"""
Builds Choropleth
"""


def choropleth_chart(file_name: str, variables: ChoroplethChartVariables):
    def get_dict(d, keys):
        for key in keys:
            d = d[key]
        return d

    def set_dict(d, keys, value):
        d = get_dict(d, keys[:-1])
        d[keys[-1]] = value
    
    df = pd.read_parquet(file_name)
    
    x = variables["x"]
    y = variables["y"]
    keys = variables["keys"] if "keys" in variables else []

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)
    df = df.replace({np.nan: None})

    res = {}
    if len(keys) < 1:
        res["x"] = df[x].tolist()
        res["y"] = {}
        for col in y:
            res["y"][col] = df[col].tolist()
        return res 

    # filter by keys 
    for key in keys:
        if df[key].dtype == "object":
            df[key] = df[key].astype(str)
        df[key] = df[key].apply(lambda x: x.lower().replace(" ", ","))
    
    # Gets all unique groups
    df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    for group in u_groups_list:
        result = {}
        for b in group[::-1]:
            result = {b: result}
        group_l = list(group)

        if len(group) == 1:
            group = group[0]
        
        x_list = df.groupby(keys)[x].get_group(group).to_list()

        chart_vals = {"x": x_list, "y": {}}

        # gets y-values for chart 
        for y_col in y:
            y_list = df.groupby(keys)[y_col].get_group(group).to_list()
            chart_vals["y"][y_col] = y_list

        final_d = chart_vals
        set_dict(result, group_l, final_d)
        merge(res, result)
        
    return res


"""
Builds Custom Chart
"""


def custom_chart(file_name: str, variables: CustomChartVariables):
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})
    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    if "district" in df.columns:  # District usually uses has spaces and Uppercase
        df["district"] = df["district"].apply(lambda x: x.lower().replace(" ", "-"))

    keys = variables["keys"]
    columns = variables["columns"]

    res = {}
    grouped = df.groupby(keys)
    for name, group in grouped:
        current_lvl = res
        if isinstance(name, tuple):
            for n in name[:-1]:
                current_lvl = current_lvl.setdefault(n, {})
            name = name[-1]
        current_lvl[name] = group[columns].to_dict('records')[0]

    return res


"""
Builds Heatmap Chart
"""


def heatmap_chart(file_name: str, variables: HeatmapChartVariables):
    cols = variables["cols"]
    id = variables["id"]
    keys = variables["keys"]
    replace_vals = variables["replace_vals"]
    dict_rename = variables["dict_rename"]
    row_format = variables["row_format"]

    df = pd.read_parquet(file_name)

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    df["id"] = df[id]
    df = df.replace({np.nan: variables["null_values"]})
    col_list = []

    if isinstance(cols, list):
        for x in cols:
            temp_str = "x_" + x
            df[temp_str] = x.title() if row_format == "title" else x.upper()
            df[x] = df[x].replace(replace_vals, regex=True)
            df["json_" + x] = (
                df[[temp_str, x]]
                .rename(columns={x: "y", temp_str: "x"})
                .apply(lambda s: s.to_dict(), axis=1)
            )
            col_list.append("json_" + x)
    else:
        rename_cols = {cols["x"]: "x", cols["y"]: "y"}
        df["json"] = (
            df[[cols["x"], cols["y"]]]
            .rename(columns=rename_cols)
            .apply(lambda s: s.to_dict(), axis=1)
        )
        col_list.append("json")

    if df["id"].dtype != "int32":
        df["id"] = df["id"].apply(lambda x: rename_labels(x, dict_rename))

    df["data"] = df[col_list].values.tolist()
    df["final"] = df[["id", "data"]].apply(lambda s: s.to_dict(), axis=1)

    df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    res = {}
    for group in u_groups_list:
        result = {}
        for b in group[::-1]:
            result = {str(b): result}
        group = group[0] if len(group) == 1 else group
        data_arr = df.groupby(keys)["final"].get_group(group).values

        cur_id = df.groupby(keys)["id"].get_group(group).unique()[0]
        cur_id = cur_id if isinstance(cur_id, str) else int(cur_id)

        group = [str(group)] if isinstance(group, str) else [str(i) for i in group]
        data_arr = (
            data_arr[0]["data"]
            if len(data_arr) == 1
            else [x["data"][0] for x in data_arr]
        )
        final_dict = {"id": cur_id, "data": data_arr}

        set_dict(result, group, final_dict, "SET")
        merge(res, result)

    return res


"""
Builds Snapshot Chart
"""


def snapshot_chart(file_name: str, variables: SnapshotChartVariables):
    df = pd.read_parquet(file_name)
    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)
    df = df.replace({np.nan: None})

    main_key = variables["main_key"]
    replace_word = variables["replace_word"]
    data = variables["data"]

    # District usually uses has spaces and Uppercase
    if "district" in df.columns and "district" not in data["data"]:
        df["district"] = df["district"].apply(lambda x: x.lower().replace(" ", "-"))

    record_list = list(data.keys())
    record_list.append("index")
    record_list.append(main_key)

    df["index"] = range(0, len(df[main_key]))

    changed_cols = {}
    for k, v in data.items():
        if replace_word != "":
            changed_cols = {x: x.replace(k, replace_word) for x in v}
        for i in v :
            if df[i].dtype == "object" :
                df[i] = df[i].astype(str)

        df[k] = df[v].rename(columns=changed_cols).apply(lambda s: s.to_dict(), axis=1)

    res_dict = df[record_list].to_dict(orient="records")

    res = []

    for i in res_dict:
        res.append(i)

    return res


"""
Builds Timeseries Chart
"""


def timeseries_chart(filename: str, variables: TimeseriesChartVariables):
    df = pd.read_parquet(filename)
    df = df.replace({np.nan: None})

    DATE_RANGE = ""
    if "DATE_RANGE" in variables:
        DATE_RANGE = variables["DATE_RANGE"]
        variables.pop("DATE_RANGE")  # Pop after use to not effect the rest of the code

    df["date"] = pd.to_datetime(df["date"])

    if "_YEARS" in DATE_RANGE:
        DATE_RANGE = int(DATE_RANGE.replace("_YEARS", ""))
        last_date = pd.Timestamp(pd.to_datetime(df["date"].max()))
        start_date = pd.Timestamp(
            pd.to_datetime(last_date) - relativedelta(years=DATE_RANGE)
        )
        df = df[(df.date >= start_date) & (df.date <= last_date)]

    df["date"] = df["date"].values.astype(np.int64) // 10**6

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)
    
    keys = variables["keys"]
    values = variables["values"]

    column_names = list(values.keys())
    rename_cols = dict(zip(list(values.values()) ,column_names))
    df = df.rename(columns=rename_cols)

    result = {}
    if len(keys) >= 1 : 
        for name, group in df.groupby(keys):
            current_level = result
            for i in range(len(name)-1):
                current_level = current_level.setdefault(name[i], {})
            current_level[name[-1]] = group[column_names].to_dict('list')
    else : 
        for i in column_names : 
            result[i] = df[i].to_list()

    return result


"""
Builds Line Chart
"""
def line_chart(file_name: str, variables):
    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})
    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    structure_info = {"key_list": [], "value_obj": []}

    get_nested_keys(variables, structure_info)
    keys_list = structure_info["key_list"][::-1]
    value_obj = structure_info["value_obj"]

    if len(keys_list) == 0:
        res = {}
        for k, v in variables.items():
            res[k] = df[v].to_list()
    else:
        df["u_groups"] = list(df[keys_list].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        res = {}
        for group in u_groups_list:
            result = {}
            for b in group[::-1]:
                result = {b: result}
            for k, v in value_obj[0].items():
                group_l = group + (k,)
                temp_group = group[0] if len(group) == 1 else group
                set_dict(
                    result,
                    list(group_l),
                    df.groupby(keys_list)[v].get_group(temp_group).to_list(),
                    "SET",
                )
            merge(res, result)

    return res


"""
Builds Waffle Chart
"""


def waffle_chart(file_name: str, variables: WaffleChartVariables):
    df = pd.read_parquet(file_name)
    df["state"].replace(STATE_ABBR, inplace=True)

    wanted = variables["wanted"]
    group = variables["groups"]
    dict_keys = variables["dict_keys"]
    data_arr = variables["data_arr"]

    key_value = []

    if len(wanted) > 0:
        df = df[df["age_group"].isin(wanted)]

    for k, v in data_arr.items():
        if isinstance(v, dict):
            key_value.append(list(v.keys())[0])
            key_value.append(list(v.values())[0])
        else:
            df[k] = df[v]

    df["data"] = df[list(data_arr.keys())].apply(lambda s: s.to_dict(), axis=1)

    df["u_groups"] = list(df[group].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    res = {}
    for groups in u_groups_list:
        result = {}
        for b in groups[::-1]:
            result = {b: result}
        cur_df = df.groupby(group)[dict_keys].get_group(groups)
        temp_df = df.groupby(group)[[key_value[0], "data"]].get_group(groups)

        temp_dict = dict(cur_df.values)
        temp_dict["data"] = (
            temp_df["data"].loc[temp_df[key_value[0]] == key_value[1]].to_list()
        )
        set_dict(result, list(groups), temp_dict, "SET")
        merge(res, result)

    return res


"""
Custom helpers for facilities
"""


def helpers_custom(file_name: str):
    df = pd.read_parquet(file_name)
    df["state"].replace(STATE_ABBR, inplace=True)

    state_mapping = {}
    state_mapping["facility_types"] = df["type"].unique().tolist()
    state_mapping["state_district_mapping"] = {}

    for state in df["state"].unique():
        state_mapping["state_district_mapping"][state] = (
            df.groupby("state").get_group(state)["district"].unique().tolist()
        )

    return state_mapping


"""
Maps Latitude and Longitudes
"""


def map_lat_lon(file_name: str, variables: LatLonVariables):
    keys = variables["keys"]
    values = variables["values"]

    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: variables["null_vals"]})

    if "state" in df.columns:
        df["state"].replace(STATE_ABBR, inplace=True)

    if "district" in df.columns:
        df["district"] = df["district"].apply(lambda x: x.lower().replace(" ", "-"))

    # Remove in the future
    if "type" in df.columns:
        df["type"] = df["type"].apply(lambda x: x.lower())

    df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    res = {}

    for group in u_groups_list:
        result = {}
        for b in group[::-1]:
            result = {b: result}
        d_values = df.groupby(keys).get_group(group)[values].to_dict(orient="records")
        set_dict(result, list(group), d_values, "SET")
        merge(res, result)

    return res


"""
Builds a jitter chart
"""


def jitter_chart(file_name: str, variables: JitterChartVariables):
    keys = variables["keys"]
    columns = variables["columns"]
    id = variables["id"]
    tooltip = variables["tooltip"]

    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})

    res = {}

    df[keys] = df[keys].apply(lambda x: x.lower().replace(" ", "_"))
    key_vals = df[keys].unique().tolist()  # Handles just 1 key ( as of now )

    for k in key_vals:
        res[k] = {}

        for key, val in columns.items():
            res[k][key] = []

            for col in val:
                x_val = col + "_x"
                y_val = col + "_y"

                cols_rename = {x_val: "x", y_val: "y", id: "id"}
                cols_inv = ["area", x_val, y_val]

                if tooltip:
                    t_val = col + "_t"
                    cols_rename[t_val] = "tooltip"
                    cols_inv.append(t_val)

                temp_df = df.groupby(keys).get_group(k)[cols_inv]
                temp_df = temp_df.rename(columns=cols_rename)
                data = temp_df.to_dict("records")
                res[k][key].append({"key": col, "data": data})

    return res


"""
Builds a pyramid chart
"""


def pyramid_chart(file_name: str, variables: PyramidChartVariables):
    col_range = variables["col_range"]
    suffix = variables["suffix"]
    keys = variables["keys"]

    df = pd.read_parquet(file_name)

    df[keys] = df[keys].apply(lambda x: x.lower().replace(" ", "_"))
    res = {}

    for k in df[keys].unique().tolist():
        res[k] = {}
        res[k]["x"] = list(col_range.keys())
        cur_df = df.groupby(keys).get_group(k)

        for s, v in suffix.items():
            s_values = [i + s for i in list(col_range.values())]
            res[k][v] = cur_df[s_values].values.tolist()[0]

    return res


"""
Builds a metrics table
"""


def metrics_table(file_name: str, variables: MetricsTableVariables):
    keys = variables["keys"]
    attr = variables["obj_attr"]
    cols = list(attr.keys())

    df = pd.read_parquet(file_name)

    df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
    u_groups_list = df["u_groups"].unique().tolist()

    res = {}
    for group in u_groups_list:
        group_key = group[0] if len(group) == 1 else group
        result = {}
        start = (
            df.groupby(keys)[cols]
            .get_group(group_key)
            .rename(columns=attr)
            .to_dict("records")
        )
        for b in group[::-1]:
            result = {b: start}
            start = result
        merge(res, result)

    return res


"""
Timeseries chart builder for values which are shared
"""


def timeseries_shared(file_name: str, variables):
    keys = variables["keys"]
    constant = variables["constant"]
    attributes = variables["attributes"]

    df = pd.read_parquet(file_name)
    df = df.replace({np.nan: None})

    df["date"] = pd.to_datetime(df["date"])
    df["date"] = df["date"].values.astype(np.int64) // 10**6

    res = {}

    for k, v in constant.items():
        res[k] = df[v].unique().tolist()

    if len(keys) == 0:
        for k, v in attributes.items():
            res[k] = df[v].to_list()
    else:
        df["u_groups"] = list(df[keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        grouped_df = df.groupby(keys)

        for grp in u_groups_list:
            temp_df = grouped_df.get_group(grp)
            val_res = {}
            for k,v in attributes.items():
                val_res[k] = temp_df[v].to_list()

            result = val_res
            for b in grp[::-1]:
                result = {b: result}
            merge(res, result)

    return res


"""
Query values chart builder for values to be queried, usually as options for dropdown selection.
"""


def query_values(file_name: str, variables: QueryValuesVariables):
    df = pd.read_parquet(file_name)
    columns = variables.get("columns", [])
    isFlat = variables.get("flat", False)
    sort_values = variables.get("sort_values", False)

    if sort_values:
        sort_cols, ascending = sort_values.get(
            "by", []), sort_values.get("ascending", [])
        df = df.sort_values(by=sort_cols, ascending=ascending)

    if len(columns) == 1:
        return list(df[columns[0]].unique())

    if isFlat:
        keys = df.drop_duplicates(subset=columns)[columns]
        return keys.to_dict(orient="records")

    res = {}
    for keys, v in df.groupby(columns[:-1])[columns[-1]]:
        d = res
        val = list(set(v))
        for k in keys:
            if k not in d:
                d[k] = {}
            parent = d
            d = d[k]
        parent[k] = val

    return res
