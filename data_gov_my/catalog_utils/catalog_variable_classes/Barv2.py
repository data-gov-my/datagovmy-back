from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import GeneralChartsUtil

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
    b_keys = []
    b_x = ""
    b_y = []

    """
    Initiailize the neccessary data for a bar chart
    """

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(self, full_meta, file_data, cur_data, all_variable_data, file_src)

        # Sets API details
        self.chart_type = self.chart['chart_type']
        self.api_filter = self.chart["chart_filters"]["SLICE_BY"]
        self.api = self.build_api_info()

        # Sets chart details
        self.b_keys = self.chart["chart_variables"]["parents"]
        self.b_x = self.chart["chart_variables"]["format"]["x"]
        self.b_y = self.get_y_values()

        # Builds the chart
        self.chart_details["chart"] = self.chart_v2()

        # Sets the catalog data within db input
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Chart builder version 2
    """
    def chart_v2(self) :
        result = {}

        df = pd.read_parquet(self.read_from)

        if 'date' in df.columns : 
            self.b_keys.insert(0, 'date')

        if len(self.b_keys) > 0 : 
            result = self.build_chart_parents()
        else : 
            result = self.build_chart_self()

        return result


    """
    Builds chart data with 0 nested keys
    """
    def build_chart_self(self) :
        df = pd.read_parquet(self.read_from)

        if "STACKED" in self.chart_type : 
            df = self.abs_to_perc(df)
        
        df = df.replace({np.nan: None}) 

        c_vals = {} # Chart Values
        t_vals = {} # Table Values

        rename_cols = {}
        rename_cols[self.b_x] = 'x'

        c_vals['x'] = df[self.b_x].to_list()

        for index, y in enumerate(self.b_y):
            y_list = df[y].to_list()
            y_val = f"y{ index + 1 }"
            rename_cols[y] = y_val
            c_vals[y_val] = y_list
        
        t_vals = (
            df.rename(columns=rename_cols)[list(rename_cols.values())]
            .to_dict("records")
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

        if "STACKED" in self.chart_type : 
            df = self.abs_to_perc(df)

        df = df.replace({np.nan: None}) 
        
        # Converts all values to : 
        # - A str if its an object
        # - A str with lowercase, and spaces as hyphen

        for key in self.b_keys:
            if df[key].dtype == "object" :
                df[key] = df[key].astype(str)            
            df[key] = df[key].apply(lambda x: x.lower().replace(" ", "-"))

        # Gets all unique groups
        df["u_groups"] = list(df[self.b_keys].itertuples(index=False, name=None))
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

            if len(group) == 1 : 
                group = group[0]
            
            x_list = df.groupby(self.b_keys)[self.b_x].get_group(group).to_list()

            rename_columns = {self.b_x: "x"} # Dict to rename columns for table
            chart_vals = {"x": x_list} # Extracted chart values

            # Gets y-values for chart
            for index, y in enumerate(self.b_y):
                y_list = df.groupby(self.b_keys)[y].get_group(group).to_list()
                y_val = f"y{index + 1}"
                rename_columns[y] = y_val
                chart_vals[y_val] = y_list

            # Gets y-values for table
            table_vals = (
                df.rename(columns=rename_columns)
                .groupby(self.b_keys)[list(rename_columns.values())]
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
    def set_table_columns(self) :
        res = {}

        res["en"] = {}
        res["bm"] = {}

        if self.table_translation:
            res['en']['x'] = self.table_translation["en"]["x"]
            res['bm']['x'] = self.table_translation["bm"]["x"]

            for y_lang in ["en", "bm"] :
                y_list = self.table_translation[y_lang]["y"]
                for index, c_y in enumerate(y_list):
                    y_val = f"y{ index + 1}"
                    res[y_lang][y_val] = c_y
        else:
            res["en"]["x"] = self.b_x
            res["bm"]["x"] = self.b_x

            for index, y in enumerate(self.b_y):
                for y_lang in ["en", "bm"]:
                    y_val = f"y{ index + 1}"
                    res[y_lang][y_val] = y

        return res       

    """
    Builds the API info for Bar
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
        res["API"]["chart_type"] = self.chart["chart_type"]

        # Builds the line config
        if self.chart_type == "LINE" : 
            line_types = self.chart['chart_variables']['line_type']
            for idx, i in enumerate(list(line_types.values())) :
                res['API']['line_variables'][f'y{idx + 1 }'] = i

        return res["API"]
    
    """
    Returns converted stacked values from abs to perc
    """
    def abs_to_perc(self, df_given):
        temp = df_given.copy()
        temp['total'] = temp[self.b_y].sum(axis=1)
        for c in self.b_y:
            temp[c] = temp[c]/temp['total']*100
        temp.drop(['total'], axis=1, inplace=True)
        return temp
    

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
    Returns the appropriate y-values
    """
    def get_y_values(self) :
        y_values = self.chart["chart_variables"]["format"]["y"]

        if isinstance(y_values, str) :
            return [y_values]
        
        return y_values