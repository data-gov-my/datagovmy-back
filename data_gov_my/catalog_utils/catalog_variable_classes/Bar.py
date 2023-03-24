from data_gov_my.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Bar(GeneralChartsUtil):
    """Bar Class for timeseries variables"""

    chart_type = ""

    # API related fields
    api_filter = []
    translations = {}

    # Chart related
    chart_name = {}
    b_keys = []
    b_x = ""
    b_y = []

    """
    Initiailize the neccessary data for a bar chart
    """

    def __init__(
        self,
        full_meta,
        file_data,
        meta_data,
        variable_data,
        all_variable_data,
        file_src,
    ):
        GeneralChartsUtil.__init__(
            self,
            full_meta,
            file_data,
            meta_data,
            variable_data,
            all_variable_data,
            file_src,
        )

        self.chart_type = meta_data["chart"]["chart_type"]
        self.api_filter = meta_data["chart"]["chart_filters"]["SLICE_BY"]
        self.precision = (
            meta_data["chart"]["chart_filters"]["precision"]
            if "precision" in meta_data["chart"]["chart_filters"]
            else 1
        )

        self.api = self.build_api_info()

        self.translations = (
            meta_data["chart"]["chart_variables"]["format_lang"]
            if "format_lang" in meta_data["chart"]["chart_variables"]
            else {}
        )
        self.b_keys = meta_data["chart"]["chart_variables"]["parents"]
        self.b_x = meta_data["chart"]["chart_variables"]["format"]["x"]
        self.b_y = meta_data["chart"]["chart_variables"]["format"]["y"]

        if isinstance(self.b_y, str):
            self.b_y = [self.b_y]

        self.chart_name = {
            "en": self.variable_data["title_en"],
            "bm": self.variable_data["title_bm"],
        }

        self.chart_details["chart"] = self.build_chart()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Build the Bar chart
    """

    def build_chart(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        for key in self.b_keys:
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        df["u_groups"] = list(df[self.b_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        overall = {}
        res = {}
        table_res = {}

        if self.translations:
            table_res["tbl_columns"] = {
                "x_en": self.translations["x_en"],
                "x_bm": self.translations["x_bm"],
                "date_en" : "Date",
                "date_bm" : "Tarikh"
            }

            for y_lang in ["en", "bm"]:
                count = 1
                for c_y in self.translations["y_" + y_lang]:
                    y_val = "y" + str(count) + "_" + y_lang
                    table_res["tbl_columns"][y_val] = c_y
                    count += 1
        else:
            table_res["tbl_columns"] = {
                "x_en": self.b_x,
                "x_bm": self.b_x,
            }

            count = 1
            for y in self.b_y:
                for y_lang in ["en", "bm"]:
                    y_val = "y" + str(count) + "_" + y_lang
                    table_res["tbl_columns"][y_val] = y
                count += 1

        for group in u_groups_list:
            result = {}
            tbl = {}
            for b in group[::-1]:
                result = {b: result}
                tbl = {b: tbl}
            group_l = [group[0]] if len(group) == 1 else list(group)
            group = group[0] if len(group) == 1 else group
            x_list = df.groupby(self.b_keys)[self.b_x].get_group(group).to_list()
            date_list = None if 'date' not in df.columns else df.groupby(self.b_keys)['date'].get_group(group).to_list()

            rename_columns = {self.b_x: "x"}
            chart_vals = {"date" : date_list, "x": x_list}
            count = 1
            for y in self.b_y:
                y_list = df.groupby(self.b_keys)[y].get_group(group).to_list()
                y_val = "y" + str(count)
                rename_columns[y] = y_val
                chart_vals[y_val] = y_list
                count += 1

            table_vals = (
                df.rename(columns=rename_columns)
                .groupby(self.b_keys)[list(rename_columns.values())]
                .get_group(group)
                .to_dict("records")
            )

            final_d = chart_vals
            self.set_dict(result, group_l, final_d)
            self.set_dict(tbl, group_l, table_vals)
            merge(res, result)
            merge(table_res, tbl)

        overall["chart_data"] = res
        overall["table_data"] = table_res
        return overall

    """
    Builds the API info for timeseries
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if self.api_filter:
            for api in self.api_filter:
                fe_vals = df[api].unique().tolist()
                be_vals = (
                    df[api]
                    .apply(lambda x: x.lower().replace(" ", "-"))
                    .unique()
                    .tolist()
                )
                api_obj = self.build_api_object_filter(
                    api, fe_vals[0], be_vals[0], dict(zip(fe_vals, be_vals))
                )
                api_filters_inc.append(api_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.meta_data["chart"]["chart_type"]

        return res["API"]
