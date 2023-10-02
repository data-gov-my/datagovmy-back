from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import (
    GeneralChartsUtil,
)

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Timeseries(GeneralChartsUtil):
    """Timeseries Class for timeseries variables"""

    chart_type = ""

    # API related fields
    api_filter = []

    # Timeseries related fields
    frequency = ""
    applicable_frequency = []
    limit_frequency = ""

    # Constant mappings
    timeseries_values = {
        "DAILY": "Daily",
        "WEEKLY": "Weekly",
        "MONTHLY": "Monthly",
        "QUARTERLY": "Quarterly",
        "YEARLY": "Yearly",
    }
    timeline = {"DAILY": 2, "WEEKLY": 5}
    group_time = {"WEEKLY": "W", "MONTHLY": "M", "QUARTERLY": "Q", "YEARLY": "Y"}

    # Chart related
    chart_name = {}
    t_keys = ""
    t_operation = ""
    t_format = ""
    t_x = ""
    t_y = []

    """
    Initiailize the neccessary data for a timeseries chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(
            self, full_meta, file_data, cur_data, all_variable_data, file_src
        )

        self.chart_type = self.chart["chart_type"]
        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]

        self.frequency = self.data_frequency
        self.limit_frequency = self.set_limit_frequency()

        self.applicable_frequency = self.get_range_values()
        self.api = self.build_api_info()

        self.t_keys = self.chart["chart_variables"]["parents"]
        self.t_format = self.chart["chart_variables"]["format"]
        self.t_x = self.chart["chart_variables"]["format"]["x"]
        self.t_y = self.get_y_values()
        self.t_operation = self.chart["chart_variables"]["operation"]

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.chart_details["chart"] = self.build_chart()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Get the y-values from meta
    """

    def get_y_values(self):
        y_vals = self.chart["chart_variables"]["format"]["y"]

        if isinstance(y_vals, list):
            return y_vals

        return [y_vals]

    """
    Set limit frequency
    """

    def set_limit_frequency(self):
        if "limit_frequency" in self.catalog_filters:
            return self.catalog_filters["limit_frequency"]

        return False

    """
    Populates the table dictionary
    """

    def populate_table_info(self, res):
        res["table_data"] = {}

        res["table_data"]["data"] = {}
        res["table_data"]["columns"] = {}

        res["table_data"]["columns"]["en"] = {}
        res["table_data"]["columns"]["en"]["x"] = "Date"

        res["table_data"]["columns"]["bm"] = {}
        res["table_data"]["columns"]["bm"]["x"] = "Tarikh"

        for index, i in enumerate(self.t_y):
            res["table_data"]["columns"]["en"][f"y{index+1}"] = i
            res["table_data"]["columns"]["bm"][f"y{index+1}"] = i

    """
    Build the timeseries chart
    """

    def build_chart(self):
        df = pd.read_parquet(self.read_from)

        for key in self.t_keys:
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        if "date" in df.columns:  # INTRADAY DOESN'T HAVE DATE
            df["date"] = pd.to_datetime(df["date"])

        isIntraday = self.chart_type == "INTRADAY"

        if ("timestamp" in df.columns) and (isIntraday):
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        if "STACKED" in self.chart_type:
            df = self.abs_to_perc(df)

        df = df.replace({np.nan: None})

        res = {}
        res["chart_data"] = {}
        res["table_data"] = {}

        if self.t_keys:
            df["u_groups"] = list(df[self.t_keys].itertuples(index=False, name=None))
            u_groups_list = df["u_groups"].unique().tolist()

            for group in u_groups_list:
                result = {}
                tbl = {}
                for b in group[::-1]:
                    result = {b: result}
                    tbl = {b: tbl}
                cur_group = group[0] if len(group) == 1 else group
                cur_data = (
                    self.build_intraday(df, cur_group)
                    if isIntraday
                    else self.slice_timeline(df, cur_group)
                )
                chart_vals = cur_data["chart_data"]
                table_vals = cur_data["table_data"]
                self.set_dict(result, list(group), chart_vals)
                self.set_dict(tbl, list(group), table_vals)
                merge(res["chart_data"], result)
                merge(res["table_data"], tbl)

            return res
        else:
            data_res = (
                self.build_intraday(df, "")
                if isIntraday
                else self.slice_timeline(df, "")
            )

            res["chart_data"] = data_res["chart_data"]
            res["table_data"] = data_res["table_data"]

            return res

    """
    Intraday Chart builder
    """

    def build_intraday(self, df, cur_group):
        df = df.copy()
        res = {}
        res["chart_data"] = {}
        res["table_data"] = {}

        df["timestamp"] = df["timestamp"].values.astype(np.int64) // 10**6

        if self.t_keys:
            res["chart_data"]["x"] = (
                df.groupby(self.t_keys)["timestamp"]
                .get_group(cur_group)
                .replace({np.nan: None})
                .to_list()
            )
            for index, y in enumerate(self.t_y):
                res["chart_data"][f"y{index + 1}"] = (
                    df.groupby(self.t_keys)[y]
                    .get_group(cur_group)
                    .replace({np.nan: None})
                    .to_list()
                )
            res["table_data"] = self.build_variable_table(res["chart_data"])
        else:
            res["chart_data"]["x"] = df["timestamp"].replace({np.nan: None}).to_list()
            for index, y in enumerate(self.t_y):
                res["chart_data"][f"y{index + 1}"] = (
                    df[y].replace({np.nan: None}).to_list()
                )
            res["table_data"] = self.build_variable_table(res["chart_data"])

        return res

    """
    If range df in timeline, return appropriate range
    """

    def is_in_timeline(self, range, range_df):
        if range in self.timeline:
            last_date = pd.Timestamp(pd.to_datetime(range_df["date"].max()))
            start_date = pd.Timestamp(
                pd.to_datetime(last_date) - relativedelta(years=self.timeline[range])
            )
            range_df = range_df[
                (range_df.date >= start_date) & (range_df.date <= last_date)
            ]
            return range_df

        return range_df

    """
    Formats the dataframe depending on the operation choice
    """

    def format_by_operation(self, df, key_list_mod):
        if self.t_operation == "SUM":
            return df.groupby(key_list_mod, as_index=False)[self.t_y].sum()
        elif self.t_operation == "MEAN":
            return df.groupby(key_list_mod, as_index=False)[self.t_y].mean()
        elif self.t_operation == "MEDIAN":
            return df.groupby(key_list_mod, as_index=False)[self.t_y].median()

        return df

    """
    Slice the timeseries based on conditions of the frequency of when data is updated
    """

    def slice_timeline(self, df, cur_group):
        res = {}

        res["chart_data"] = {}
        res["table_data"] = {}

        for range in self.applicable_frequency:
            res["chart_data"][range] = {}  # Creates the default key for this range
            range_df = df.copy()  # Creates the copy of this dataframe

            range_df = self.is_in_timeline(range, range_df)

            if range in self.group_time:  # If requires time grouping
                key_list_mod = self.t_keys[:] if self.t_keys else []
                key_list_mod.append("interval")

                range_df["interval"] = range_df["date"]

                if self.frequency != "WEEKLY":
                    range_df["interval"] = (
                        range_df["date"]
                        .dt.to_period(self.group_time[range])
                        .dt.to_timestamp()
                    )

                range_df["interval"] = (
                    range_df["interval"].values.astype(np.int64) // 10**6
                )
                new_temp_df = range_df.copy()

                # Returns df based on operation choice
                new_temp_df = self.format_by_operation(range_df, key_list_mod)

                if self.t_keys:
                    res["chart_data"][range]["x"] = (
                        new_temp_df.groupby(self.t_keys)["interval"]
                        .get_group(cur_group)
                        .replace({np.nan: None})
                        .to_list()
                    )

                    for index, y in enumerate(self.t_y):
                        res["chart_data"][range][f"y{index + 1}"] = (
                            new_temp_df.groupby(self.t_keys)[y]
                            .get_group(cur_group)
                            .replace({np.nan: None})
                            .to_list()
                        )

                else:
                    res["chart_data"][range]["x"] = (
                        new_temp_df["interval"].replace({np.nan: None}).to_list()
                    )

                    for index, y in enumerate(self.t_y):
                        res["chart_data"][range][f"y{index + 1}"] = (
                            new_temp_df[y].replace({np.nan: None}).to_list()
                        )

                res["table_data"][range] = self.build_variable_table(
                    res["chart_data"][range]
                )
            else:
                range_df["date"] = range_df["date"].values.astype(np.int64) // 10**6

                if self.t_keys:
                    res["chart_data"][range]["x"] = (
                        range_df.groupby(self.t_keys)["date"]
                        .get_group(cur_group)
                        .replace({np.nan: None})
                        .to_list()
                    )
                    for index, y in enumerate(self.t_y):
                        res["chart_data"][range][f"y{index + 1}"] = (
                            range_df.groupby(self.t_keys)[y]
                            .get_group(cur_group)
                            .replace({np.nan: None})
                            .to_list()
                        )
                    res["table_data"][range] = self.build_variable_table(
                        res["chart_data"][range]
                    )
                else:
                    res["chart_data"][range]["x"] = (
                        range_df["date"].replace({np.nan: None}).to_list()
                    )
                    for index, y in enumerate(self.t_y):
                        res["chart_data"][range][f"y{index + 1}"] = (
                            range_df[y].replace({np.nan: None}).to_list()
                        )
                    res["table_data"][range] = self.build_variable_table(
                        res["chart_data"][range]
                    )

        return res

    """
    Gets the possible range of values for the dataset
    """

    def get_range_values(self):
        pos = ["DAILY", "WEEKLY", "MONTHLY", "QUARTERLY", "YEARLY"]

        if self.limit_frequency:
            return [self.frequency]

        index = pos.index(self.frequency)
        return pos[index:]

    """
    Builds the API info for timeseries
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if self.api_filter:
            default_key = []
            for idx, api in enumerate(self.api_filter):
                filter_obj = None
                df[api] = df[api].apply(lambda x: x.lower().replace(" ", "-"))
                if idx == 0:
                    be_vals = df[api].unique().tolist()
                    default_key.append(be_vals[0])
                    filter_obj = self.build_api_object_filter(api, be_vals[0], be_vals)
                else:
                    dropdown = self.dropdown_options(
                        df, groupby_cols=self.api_filter[0:idx], column=api
                    )
                    cur_level = dropdown
                    for dk in default_key:
                        cur_level = cur_level[dk]
                    def_key = cur_level[0]
                    filter_obj = self.build_api_object_filter(
                        key=api, def_val=def_key, options=dropdown
                    )
                    default_key.append(def_key)

                api_filters_inc.append(filter_obj)

        if self.chart_type != "INTRADAY":
            range_options = [r for r in self.applicable_frequency]
            range_obj = self.build_api_object_filter(
                "range", self.frequency, range_options
            )
            api_filters_inc.append(range_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.chart["chart_type"]

        return res["API"]

    """
    Returns converted stacked values from abs to perc
    """

    def abs_to_perc(self, df_given):
        temp = df_given.copy()
        temp["total"] = temp[self.t_y].sum(axis=1)
        for c in self.t_y:
            temp[c] = temp[c] / temp["total"] * 100
        temp.drop(["total"], axis=1, inplace=True)
        return temp

    """
    Validates the meta json
    """

    def validate_meta_json(self):
        src = self.variable_name

        self.validate_field_presence(
            ["id", "name", "title_en", "title_bm", "desc_en", "desc_bm"],
            src,
            self.variable_data,
        )
        s = {
            "int": ["id"],
            "str": ["name", "title_en", "title_bm", "desc_en", "desc_bm"],
        }
        self.validate_data_type(s, src, self.variable_data)

        self.validate_field_presence(
            ["id", "catalog_filters", "metadata_neutral", "metadata_lang", "chart"],
            src,
            self.meta_data,
        )
        s = {
            "int": ["id"],
            "dict": ["catalog_filters", "metadata_neutral", "metadata_lang", "chart"],
        }
        self.validate_data_type(s, src, self.meta_data)

        self.validate_field_presence(
            ["frequency", "geography", "start", "end", "data_source"],
            src,
            self.meta_data["catalog_filters"],
        )
        s = {"str": ["frequency"], "list": ["geography", "data_source"]}
        self.validate_data_type(s, src, self.meta_data["catalog_filters"])

        self.validate_field_presence(
            ["data_as_of", "last_updated", "next_update"],
            src,
            self.meta_data["metadata_neutral"],
        )
        s = {"str": ["data_as_of", "last_updated", "next_update"]}
        self.validate_data_type(s, src, self.meta_data["metadata_neutral"])

        self.validate_field_presence(
            ["methodology", "caveat"], src, self.meta_data["metadata_lang"]["en"]
        )
        self.validate_field_presence(
            ["methodology", "caveat"], src, self.meta_data["metadata_lang"]["bm"]
        )
        s = {"str": ["methodology", "caveat"]}
        self.validate_data_type(s, src, self.meta_data["metadata_lang"]["en"])
        self.validate_data_type(s, src, self.meta_data["metadata_lang"]["bm"])

        self.validate_field_presence(
            ["chart_type", "chart_filters", "chart_variables"],
            src,
            self.meta_data["chart"],
        )
        s = {"str": ["chart_type"]}
        self.validate_data_type(s, src, self.meta_data["chart"])

        self.validate_field_presence(
            ["SLICE_BY"], src, self.meta_data["chart"]["chart_filters"]
        )
        s = {"list": ["SLICE_BY"]}
        self.validate_data_type(s, src, self.meta_data["chart"]["chart_filters"])

        self.validate_field_presence(
            ["parents", "operation", "format"],
            src,
            self.meta_data["chart"]["chart_variables"],
        )
        s = {"str": ["operation"], "list": ["parents"], "dict": ["format"]}
        self.validate_data_type(s, src, self.meta_data["chart"]["chart_variables"])

        self.validate_field_presence(
            ["x", "y"],
            src,
            self.meta_data["chart"]["chart_variables"]["format"],
        )
        s = {"str": ["x", "y"]}
        self.validate_data_type(
            s, src, self.meta_data["chart"]["chart_variables"]["format"]
        )
