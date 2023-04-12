from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Geopoint(GeneralChartsUtil):
    """Geopoint Class for choropleth variables"""

    chart_type = ""

    # API related fields
    api_filter = []

    # Choropleth Variables
    g_keys = []
    g_include = []

    """
    Initiailize the neccessary data for a Choropleth chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, cur_data, all_variable_data, file_src)

        self.chart_type = self.chart["chart_type"]

        self.g_keys = self.chart["chart_variables"]["parents"]
        self.g_include = self.chart["chart_variables"]["include"]

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
    def chart_v2(self) :
        result = {}

        df = pd.read_parquet(self.read_from)

        if 'date' in df.columns : 
            self.g_keys.insert(0, 'date')

        if len(self.g_keys) > 0 : 
            result = self.build_chart_parents()
        else : 
            result = self.build_chart_self()

        return result

    """
    Builds chart data with 0 nested keys
    """
    def build_chart_self(self) :
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})        

        include = list(self.g_include)

        df["position"] = df[["lat", "lon"]].values.tolist() # Lat, Lon, must be present
        include.append("position") # Add position into the array
        c_vals = df[include].to_dict(orient="records")

        t_columns = self.set_table_columns()
        
        # Remove position, & add lat, lon instead
        include.remove("position")
        include.append("lat")
        include.append("lon")

        t_vals = df[include].to_dict("records")      

        overall = {}
        overall["chart_data"] = c_vals
        overall["table_data"] = {}
        overall["table_data"]["columns"] = t_columns
        overall["table_data"]["data"] = t_vals  

        return overall

    """
    Build the Geopoint
    """

    def build_chart_parents(self):
        df = pd.read_parquet(self.read_from)
        df = df.replace({np.nan: None})
        df["position"] = df[["lat", "lon"]].values.tolist() # Lat, Lon, must be present

        chart_include = list(self.g_include)
        chart_include.append("position")

        table_include = list(self.g_include)
        table_include.append("lat")
        table_include.append("lon")

        # Converts all values to : 
        # - A str if its an object
        # - A str with lowercase, and spaces as hyphen

        for key in self.g_keys:
            if df[key].dtype == "object" :
                df[key] = df[key].astype(str)            
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        # Gets all unique groups
        df["u_groups"] = list(df[self.g_keys].itertuples(index=False, name=None))
        u_groups_list = df["u_groups"].unique().tolist()

        chart_res = {}
        table_res = {}

        table_columns = self.set_table_columns()

        for group in u_groups_list:
            result = {}
            tbl = {}
            for b in group[::-1]:
                result = {b: result}
                tbl = {b: tbl}
            group_l = list(group)

            if len(group) == 1 : 
                group = group[0]
            
            chart_vals = df.groupby(self.g_keys)[chart_include].get_group(group).to_dict(orient="records")
            table_vals = df.groupby(self.g_keys)[table_include].get_group(group).to_dict(orient="records")

            final_d = chart_vals
            self.set_dict(result, group_l, final_d)
            self.set_dict(tbl, group_l, table_vals)
            merge(chart_res, result)
            merge(table_res, tbl)

        overall = {}
        overall["chart_data"] = chart_res
        overall["table_data"] = {}
        overall["table_data"]["columns"] = table_columns
        overall["table_data"]["data"] = table_res 

        return overall

    """
    Set table columns
    """
    def set_table_columns(self) :
        res = {}

        res["en"] = {}
        res["bm"] = {}

        if self.table_translation: 
            
            for l in ["en", "bm"] :
                if l in self.table_translation : 
                    for k, v in self.table_translation[l].items() :
                        res[l][k] = v

        else: # Sets the default
            include = list(self.g_include)
            include.append("lat")
            include.append("lon")
            
            for l in ["en", "bm"] :
                for i in include : 
                    res[l][i] = i
        return res

    """
    Builds the API info for Choropleth
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
                df = pd.read_parquet(self.read_from)
                fe_vals = df[api].unique().tolist()
                be_vals = (
                    df[api]
                    .apply(lambda x: x.lower().replace(" ", "-"))
                    .unique()
                    .tolist()
                )
                filter_obj = self.build_api_object_filter(
                    api, fe_vals[0], be_vals[0], dict(zip(fe_vals, be_vals))
                )
                api_filters_inc.append(filter_obj)

        res["API"] = {}
        res["API"]["filters"] = api_filters_inc
        res["API"]["precision"] = self.precision
        res["API"]["chart_type"] = self.chart["chart_type"]

        return res["API"]

    """
    Builds date slider object
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