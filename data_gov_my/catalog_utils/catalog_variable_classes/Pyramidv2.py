from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Pyramid(GeneralChartsUtil):
    """Pyramid Class for timeseries variables"""

    chart_type = "PYRAMID"

    # API related fields
    api_filter = []
    translations = {}

    # Chart related
    chart_name = {}
    p_keys = []
    p_x = ""
    p_y = []

    """
    Initiailize the neccessary data for a bar chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, cur_data, all_variable_data, file_src)

        self.chart_type = self.chart["chart_type"]
        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]

        self.api = self.build_api_info()

        self.p_keys = self.chart["chart_variables"]["parents"]
        self.p_x = self.chart["chart_variables"]["x"]
        self.p_y = self.chart["chart_variables"]["y"]

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.metadata = self.rebuild_metadata()
        self.chart_details["chart"] = self.chart_v2()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Chart builder version 2
    """
    def chart_v2(self) :
        result = {}
        has_date = False

        df = pd.read_parquet(self.read_from)

        if 'date' in df.columns : 
            self.p_keys.insert(0, 'date')
            has_date = True

        if len(self.p_keys) > 0 : 
            result = self.build_chart_parents(has_date)
        else : 
            result = self.build_chart_self()

        return result

    """
    Populates table columns
    """
    def set_table_columns(self) : 
        tbl_res = {}
        tbl_res["en"] = {}
        tbl_res["bm"] = {}

        if self.table_translation:
            tbl_res["en"]["x"] = self.table_translation["en"]["x"]
            tbl_res["bm"]["x"] = self.table_translation["bm"]["x"]
            
            for y_lang in ["en", "bm"]:
                for index, c_y in enumerate(self.table_translation[y_lang]["y"]):
                    y_val = f"y{ index + 1 }"
                    tbl_res[y_lang][y_val] = c_y
        else:
            tbl_res["en"]["x"] = self.p_x
            tbl_res["bm"]["x"] = self.p_x

            for index, c_y in enumerate(self.p_y):
                for y_lang in ["en", "bm"]:
                    y_val = f"y{ index + 1 }"
                    tbl_res[y_lang][y_val] = c_y

        return tbl_res

    """
    Build chart self
    """
    def build_chart_self(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        for key in self.p_keys:
            df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        res = {}
        tbl_res = {}

        # tbl_res['data'] = {}
        # tbl_res['columns'] = self.set_table_columns()

        rename_columns = {self.p_x: "x", self.p_y[0]: "y1", self.p_y[1]: "y2"}
        x_list = df[self.p_x].to_list()
        y1_list = [x * -1 for x in df[self.p_y[0]].to_list()]
        y2_list = df[self.p_y[1]].to_list()
        res = {"x": x_list, "y1": y1_list, "y2": y2_list}
        tbl_res = df.rename(columns=rename_columns)[list(rename_columns.values())].to_dict("records")

        overall = {}
        overall["chart_data"] = res
        overall["table_data"] = tbl_res
        return overall

    """
    Build chart parents
    """
    def build_chart_parents(self, has_date) : 
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})

        for key in self.p_keys:
            df[key] = df[key].astype(str)
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        res = {}
        tbl_res = {}

        # tbl_res['data'] = {}
        # tbl_res['columns'] = self.set_table_columns()

        rename_columns = {self.p_x: "x", self.p_y[0]: "y1", self.p_y[1]: "y2"}

        df["u_groups"] = list(df[self.p_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        for group in u_groups_list:
            result = {}
            tbl = {}
            for b in group[::-1]:
                result = {b: result}
                tbl = {b: tbl}
            group_l = [group[0]] if len(group) == 1 else list(group)
            group = group[0] if len(group) == 1 else group
            x_list = df.groupby(self.p_keys)[self.p_x].get_group(group).to_list()

            y1_list = [
                x * -1
                for x in df.groupby(self.p_keys)[self.p_y[0]]
                .get_group(group)
                .to_list()
            ]
            y2_list = (
                df.groupby(self.p_keys)[self.p_y[1]].get_group(group).to_list()
            )

            table_vals = (
                df.rename(columns=rename_columns)
                .groupby(self.p_keys)[list(rename_columns.values())]
                .get_group(group)
                .to_dict("records")
            )

            chart_data = {"x": x_list, "y1": y1_list, "y2": y2_list}
            self.set_dict(result, group_l, chart_data)
            self.set_dict(tbl, group_l, table_vals)
            merge(res, result)
            merge(tbl_res, tbl)

        overall = {}
        overall["chart_data"] = res
        overall["table_data"] = tbl_res
        return overall

    """
    Builds the API info for timeseries
    """

    def build_api_info(self):
        res = {}

        df = pd.read_parquet(self.read_from)
        api_filters_inc = []

        if 'date' in df.columns :
            slider_obj = self.build_date_slider(df)
            api_filters_inc.append(slider_obj)

        if self.api_filter:
            default_key = []
            for idx, api in enumerate(self.api_filter):
                filter_obj = None
                df[api] = df[api].apply(lambda x: x.lower().replace(" ", "-"))
                if idx == 0 :                
                    be_vals = (
                        df[api]
                        .unique()
                        .tolist()
                    )
                    default_key.append(be_vals[0])
                    filter_obj = self.build_api_object_filter(api, be_vals[0], be_vals)
                else : 
                    dropdown = self.dropdown_options(df, groupby_cols=self.api_filter[0:idx], column=api)
                    cur_level = dropdown
                    for dk in default_key : 
                        cur_level = dropdown[dk]
                    def_key = cur_level[0]
                    filter_obj = self.build_api_object_filter(key=api, def_val=def_key, options=dropdown)
                    default_key.append(def_key)

                api_filters_inc.append(filter_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.chart["chart_type"]

        return res["API"]

    """
    Builds the date slider
    """

    def build_date_slider(self, df) :
        df["date"] = df["date"].astype(str)
        options_list = df["date"].unique().tolist()
        
        obj = {}
        obj['key'] = "date_slider"
        obj["default"] = options_list[0]
        obj["options"] = options_list 
        obj["interval"] = self.data_frequency

        return obj      


    """
    REBUILDS THE METADATA
    """

    def rebuild_metadata(self):
        self.metadata.pop("in_dataset", None)

        refresh_metadata = []

        for i in self.all_variable_data:
            if i["id"] != 0:
                i.pop("unique_id", None)
                refresh_metadata.append(i)

        self.metadata["out_dataset"] = refresh_metadata
        return self.metadata
