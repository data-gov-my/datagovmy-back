from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import (
    GeneralChartsUtil,
)

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge
import re


class Heattable(GeneralChartsUtil):
    """Heattable Class for timeseries variables"""

    chart_type = "HEATTABLE"

    # API related fields
    api_filter = []

    # Chart related
    chart_name = {}
    h_keys = []
    h_color = ""

    """
    Initiailize the neccessary data for a bar chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(
            self, full_meta, file_data, cur_data, all_variable_data, file_src
        )

        self.chart_type = self.chart["chart_type"]
        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]
        self.h_color = self.chart["chart_variables"]["colour"]
        self.api = self.build_api_info()

        self.h_keys = self.chart["chart_variables"]["parents"]

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.chart_details["chart"] = self.chartv2()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Chart version 2
    """

    def chartv2(self):
        result = {}

        df = pd.read_parquet(self.read_from)

        if "date" in df.columns:
            self.h_keys.insert(0, "date")

        if len(self.h_keys) > 0:
            result = self.build_chart_parents()
        else:
            result = self.build_chart_self()

        return result

    """
    Build chart self
    """

    def build_chart_self(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        df = df.rename(columns={"columns": "x", "index": "y", "values": "z"})

        group_keys = ["x", "y", "z"]

        return df[group_keys].to_dict(orient="records")

    """
    Builds chart parents
    """

    def build_chart_parents(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        df = df.rename(columns={"columns": "x", "index": "y", "values": "z"})

        for key in self.h_keys:
            if df[key].dtype == "object":
                df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        group_keys = ["x", "y", "z"]

        df["u_groups"] = list(df[self.h_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        res = {}

        for group in u_groups_list:
            result = {}
            for b in group[::-1]:
                result = {b: result}
            group_l = list(group)

            if len(group) == 1:
                group = group[0]

            final_d = (
                df.groupby(self.h_keys)[group_keys]
                .get_group(group)
                .to_dict(orient="records")
            )

            self.set_dict(result, group_l, final_d)
            merge(res, result)

        return res

    """
    Builds the date slider
    """

    def build_date_slider(self, df):
        df["date"] = df["date"].astype(str)
        options_list = df["date"].unique().tolist()

        obj = {}
        obj["key"] = "date_slider"
        obj["default"] = options_list[0]
        obj["options"] = options_list
        obj["interval"] = self.data_frequency

        return obj

    """
    Builds the API info for timeseries
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if "date" in df.columns:
            slider_obj = self.build_date_slider(df)
            api_filters_inc.append(slider_obj)

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
                        cur_level = dropdown[dk]
                    def_key = cur_level[0]
                    filter_obj = self.build_api_object_filter(
                        key=api, def_val=def_key, options=dropdown
                    )
                    default_key.append(def_key)

                api_filters_inc.append(filter_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["colour"] = self.h_color
        res["API"]["chart_type"] = self.chart["chart_type"]

        return res["API"]
