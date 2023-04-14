from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Table(GeneralChartsUtil):
    """Table Class for table variables"""

    chart_type = "TABLE"

    # Table Variables
    exclude = []
    freeze = []
    t_keys = []

    """
    Initiailize the neccessary data for a table chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, cur_data, all_variable_data, file_src)

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.exclude = self.chart["chart_variables"]["exclude"]
        self.freeze = self.chart["chart_variables"]["freeze"]
        self.t_keys = self.chart["chart_variables"]["parents"]
        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]

        self.metadata = self.rebuild_metadata()
        self.api = self.build_api_info()

        self.chart_details["chart"] = self.chartv2()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Chart version 2
    """
    def chartv2(self) :
        result = {}

        df = pd.read_parquet(self.read_from)

        if 'date' in df.columns : 
            self.t_keys.insert(0, 'date')

        if len(self.t_keys) > 0 : 
            result = self.build_chart_parents()
        else : 
            result = self.build_chart_self()

        return result

    """
    Build chart self
    """
    def build_chart_self(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        all_columns = df.columns.to_list()
        group_keys = list(set(all_columns) - set(self.exclude))
        
        # table_columns = self.set_table_columns(group_keys)
        table_data = df[group_keys].to_dict(orient="records")
        
        overall = table_data
        # overall["columns"] = table_columns
        
        return overall

    """
    Builds chart parents
    """
    def build_chart_parents(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        all_columns = df.columns.to_list()
        group_keys = list(set(all_columns) - set(self.exclude))

        for key in self.t_keys:
            if df[key].dtype == "object" :
                df[key] = df[key].astype(str)            
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        # table_columns = self.set_table_columns(group_keys)

        df["u_groups"] = list(df[self.t_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()
        
        res = {}

        for group in u_groups_list:
            result = {}
            for b in group[::-1]:
                result = {b: result}
            group_l = list(group)

            if len(group) == 1 : 
                group = group[0]            
            
            final_d = df.groupby(self.t_keys)[group_keys].get_group(group).to_dict(orient="records")

            self.set_dict(result, group_l, final_d)
            merge(res, result)

        overall = res

        return overall

    def set_table_columns(self, columns) :
        res = {}

        res["en"] = {}
        res["bm"] = {}

        if self.table_translation:
            for lang in ["en", "bm"] :
                for k, v in self.table_translation[lang] :
                    res[lang][k] = v            
        else:
            for lang in ["en", "bm"] :
                for i in columns : 
                    res[lang][i] = i

        return res 


    """
    Build the Table chart
    """

    def build_chart(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        EXCLUDE = self.exclude

        res = {}

        if EXCLUDE:
            df = df.drop(EXCLUDE, axis=1)

        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

        res["data"] = df.to_dict("records")
        res["columns"] = {"en": {}, "bm": {}}

        for obj in self.all_variable_data[1:]:
            if obj["name"] not in self.exclude:
                res["columns"]["en"][obj["name"]] = obj["title_en"]
                res["columns"]["bm"][obj["name"]] = obj["title_bm"]

        return res

    """
    Rebuilds the metadata
    """
    def rebuild_metadata(self):
        self.metadata.pop("in_dataset", None)

        refresh_metadata = []

        for i in self.all_variable_data:
            if i["id"] != 0 and i["name"] not in self.exclude:
                i.pop("unique_id", None)
                refresh_metadata.append(i)

        self.metadata["out_dataset"] = refresh_metadata
        return self.metadata

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
            for api in self.api_filter:
                be_vals = (
                    df[api]
                    .apply(lambda x: x.lower().replace(" ", "-"))
                    .unique()
                    .tolist()
                )
                api_obj = self.build_api_object_filter(api, be_vals[0], be_vals)
                api_filters_inc.append(api_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["freeze"] = self.freeze
        res["API"]["chart_type"] = self.chart["chart_type"]

        return res["API"]