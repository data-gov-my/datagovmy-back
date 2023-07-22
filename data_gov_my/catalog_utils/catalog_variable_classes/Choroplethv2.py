from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import (
    GeneralChartsUtil,
)

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Choropleth(GeneralChartsUtil):
    """Choropleth Class for choropleth variables"""

    chart_type = ""

    # API related fields
    api_filter = []

    # Choropleth Variables
    c_keys = []
    c_x = ""
    c_y = ""
    c_color = ""
    c_file_json = ""

    """
    Initiailize the neccessary data for a Choropleth chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(
            self, full_meta, file_data, cur_data, all_variable_data, file_src
        )

        self.chart_type = self.chart["chart_type"]

        self.c_color = self.chart["chart_variables"]["colour"]
        self.c_file_json = self.chart["chart_variables"]["file_json"]
        self.c_keys = self.chart["chart_variables"]["parents"]
        self.c_x = self.chart["chart_variables"]["format"]["x"]
        self.c_y = self.chart["chart_variables"]["format"]["y"]

        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]
        self.api = self.build_api_info()

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.chart_details["chart"] = self.chart_v2()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Chart builder version 2
    """

    def chart_v2(self):
        result = {}

        df = pd.read_parquet(self.read_from)

        if "date" in df.columns:
            self.c_keys.insert(0, "date")

        if len(self.c_keys) > 0:
            result = self.build_chart_parents()
        else:
            result = self.build_chart_self()

        return result

    """
    Builds chart data with 0 nested keys
    """

    def build_chart_self(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        c_vals = {}  # Chart Values
        t_vals = {}  # Table Values

        # t_columns = self.set_table_columns()

        rename_cols = {}
        rename_cols[self.c_x] = "x"

        c_vals["x"] = df[self.c_x].to_list()

        y_list = df[self.c_y].to_list()
        rename_cols[self.c_y] = "y"
        c_vals["y"] = y_list

        t_vals = df.rename(columns=rename_cols)[list(rename_cols.values())].to_dict(
            "records"
        )

        overall = {}
        overall["chart_data"] = c_vals
        overall["table_data"] = t_vals

        return overall

    """
    Build the Bar chart
    """

    def build_chart_parents(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        # Converts all values to :
        # - A str if its an object
        # - A str with lowercase, and spaces as hyphen

        for key in self.c_keys:
            if df[key].dtype == "object":
                df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        # Gets all unique groups
        df["u_groups"] = list(df[self.c_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        chart_res = {}
        table_res = {}

        # table_columns = self.set_table_columns()

        for group in u_groups_list:
            result = {}
            tbl = {}
            for b in group[::-1]:
                result = {b: result}
                tbl = {b: tbl}
            group_l = list(group)

            if len(group) == 1:
                group = group[0]

            x_list = df.groupby(self.c_keys)[self.c_x].get_group(group).to_list()

            rename_columns = {self.c_x: "x"}  # Dict to rename columns for table
            chart_vals = {"x": x_list}  # Extracted chart values

            # Gets y-values for chart
            y_list = df.groupby(self.c_keys)[self.c_y].get_group(group).to_list()
            rename_columns[self.c_y] = "y"
            chart_vals["y"] = y_list

            # Gets y-values for table
            table_vals = (
                df.rename(columns=rename_columns)
                .groupby(self.c_keys)[list(rename_columns.values())]
                .get_group(group)
                .to_dict("records")
            )

            final_d = chart_vals
            self.set_dict(result, group_l, final_d)
            self.set_dict(tbl, group_l, table_vals)
            merge(chart_res, result)
            merge(table_res, tbl)

        overall = {}
        overall["chart_data"] = chart_res
        overall["table_data"] = table_res

        return overall

    """
    Set table columns
    """

    def set_table_columns(self):
        res = {}

        res["en"] = {}
        res["bm"] = {}

        if self.table_translation:
            res["en"]["x"] = self.table_translation["en"]["x"]
            res["en"]["y"] = self.table_translation["en"]["y"]

            res["bm"]["x"] = self.table_translation["bm"]["x"]
            res["bm"]["y"] = self.table_translation["bm"]["y"]
        else:
            res["en"]["x"] = self.c_x
            res["en"]["y"] = self.c_y

            res["bm"]["x"] = self.c_x
            res["bm"]["y"] = self.c_y

        return res

    """
    Builds the API info for Choropleth
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)

        api_filters_inc = []

        if "date" in df.columns:
            slider_obj = self.build_date_slider(df)
            api_filters_inc.append(slider_obj)

        if self.api_filter:
            for api in self.api_filter:
                df = pd.read_parquet(self.read_from)
                be_vals = (
                    df[api]
                    .apply(lambda x: x.lower().replace(" ", "-"))
                    .unique()
                    .tolist()
                )
                filter_obj = self.build_api_object_filter(api, be_vals[0], be_vals)
                api_filters_inc.append(filter_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.chart["chart_type"]
        res["API"]["colour"] = self.c_color
        res["API"]["file_json"] = self.c_file_json

        return res["API"]

    """
    Builds date slider object
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
    Validates Meta Json
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
            ["parents", "color", "file_json", "format"],
            src,
            self.meta_data["chart"]["chart_variables"],
        )
        s = {"str": ["color", "file_json"], "list": ["parents"], "dict": ["format"]}
        self.validate_data_type(s, src, self.meta_data["chart"]["chart_variables"])

        self.validate_field_presence(
            ["x", "y"], src, self.meta_data["chart"]["chart_variables"]["format"]
        )
        s = {"str": ["x", "y"]}
        self.validate_data_type(
            s, src, self.meta_data["chart"]["chart_variables"]["format"]
        )
