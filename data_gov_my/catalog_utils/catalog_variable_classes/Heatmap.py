from data_gov_my.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge
import re


class Heatmap(GeneralChartsUtil):
    """Heatmap Class for timeseries variables"""

    chart_type = "HEATMAP"

    # API related fields
    api_filter = []

    # Chart related
    chart_name = {}
    h_keys = []
    h_id = ""
    h_cols = []

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

        self.h_keys = meta_data["chart"]["chart_variables"]["parents"]
        self.h_id = meta_data["chart"]["chart_variables"]["id"]
        self.h_cols = meta_data["chart"]["chart_variables"]["cols"]

        self.chart_name = {
            "en": self.variable_data["title_en"],
            "bm": self.variable_data["title_bm"],
        }

        self.chart_details["chart"] = self.build_chart()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Build the Heatmap chart
    """

    def build_chart(self):
        df = pd.read_parquet(self.read_from)

        df["id"] = df[id]
        df = df.replace({np.nan: None})
        col_list = []

        if isinstance(self.h_cols, list):
            for x in self.h_cols:
                temp_str = "x_" + x
                df[temp_str] = x.title()
                df[x] = df[x].replace({}, regex=True)  # replace_vals here
                df["json_" + x] = (
                    df[[temp_str, x]]
                    .rename(columns={x: "y", temp_str: "x"})
                    .apply(lambda s: s.to_dict(), axis=1)
                )
                col_list.append("json_" + x)
        else:
            rename_cols = {self.h_cols["x"]: "x", self.h_cols["y"]: "y"}
            df["json"] = (
                df[[self.h_cols["x"], self.h_cols["y"]]]
                .rename(columns=rename_cols)
                .apply(lambda s: s.to_dict(), axis=1)
            )
            col_list.append("json")

        if df["id"].dtype != "int32":
            df["id"] = df["id"].apply(
                lambda x: self.rename_labels(x, {})
            )  # dict_rename here

        df["data"] = df[col_list].values.tolist()
        df["final"] = df[["id", "data"]].apply(lambda s: s.to_dict(), axis=1)

        df["u_groups"] = list(df[self.h_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        res = {}
        for group in u_groups_list:
            result = {}
            for b in group[::-1]:
                result = {str(b): result}
            group = group[0] if len(group) == 1 else group
            data_arr = df.groupby(self.h_keys)["final"].get_group(group).values

            cur_id = df.groupby(self.h_keys)["id"].get_group(group).unique()[0]
            cur_id = cur_id if isinstance(cur_id, str) else int(cur_id)

            group = [str(group)] if isinstance(group, str) else [str(i) for i in group]
            data_arr = (
                data_arr[0]["data"]
                if len(data_arr) == 1
                else [x["data"][0] for x in data_arr]
            )
            final_dict = {"id": cur_id, "data": data_arr}

            self.set_dict(result, group, final_dict, "SET")
            merge(res, result)

        return res

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

    """
    Rename labels
    """

    def rename_labels(label, rname_dict):
        for k, v in rname_dict.items():
            label = re.sub(k, v, label)

        return label.replace("_", " ").title()
