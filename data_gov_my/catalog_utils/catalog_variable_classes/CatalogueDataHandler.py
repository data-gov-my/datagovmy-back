from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class CatalogueDataHandler() :

    table_view_attributes = {
        "HEATTABLE" : ["x", "y", "z"]
    }

    
    """
    Constructor
    """
    def __init__(self, chart_type, data, params) :
        self._chart_type = chart_type
        self._data = data
        self._params = params

    """
    Gets the results depending on which chart type
    """
    def get_results(self) :
        # Families with same format
        if self._chart_type in ['BAR','HBAR','STACKED_BAR', 'TIMESERIES', 'PYRAMID', 'CHOROPLETH', 'GEOCHOROPLETH', "GEOPOINT", "SCATTER"] :
            return self.array_value_handler()
        elif self._chart_type in ['HEATTABLE'] :
            return self.table_view_handler()
        elif self._chart_type in ['GEOJSON'] :
            return self.geo_data_handler()
        
    """
    Handles chart type Geodata
    """
    def geo_data_handler(self) :
        lang = self.default_param_value('lang', 'en', self._params)

        res = {}
        intro = self._data["chart_details"]["intro"]
        self.extract_lang(lang)

        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res
        return self._data


    """
    Handles charts with a table view, E.g : 
    1. Heatmap
    """
    def table_view_handler(self) :
        lang = self.default_param_value('lang', 'en', self._params)

        intro = self._data["chart_details"]["intro"]
        chart_data = self._data["chart_details"]["chart"] # should put as chart_data(?)

        defaults_api = {}

        for d in self._data["API"]["filters"]: # Gets all the default API values
            if d["key"] == "date_slider" : 
                defaults_api[d["key"]] = d["default"]
            else : 
                defaults_api[d["key"]] = d["default"]["value"]

        for k, v in defaults_api.items():
            key = self._params[k][0] if k in self._params else v
            if key in chart_data :
                chart_data = chart_data[key]
            else:
                chart_data = {}
                break

        self.extract_lang(lang)

        chart_data = self.extract_lang_table_view(lang, self.table_view_attributes[self._chart_type], chart_data)
        res = {}
        res["chart_data"] = chart_data
        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res

        return self._data

    '''
    This handler supports of type : 
    1. Bar : BAR, HBAR, STACKED_BAR
    2. Timeseries : TIMESERIES, AREA, STACKED_AREA
    '''
    def array_value_handler(self) :
        lang = self.default_param_value('lang', 'en', self._params)

        intro = self._data["chart_details"]["intro"]  # Get intro
        table_data = self._data["chart_details"]["chart"]["table_data"]["data"]
        table_cols = self._data["chart_details"]["chart"]["table_data"]["columns"]
        chart_data = self._data["chart_details"]["chart"]["chart_data"]  # Get chart data

        defaults_api = {} # Creates a default API

        for d in self._data["API"]["filters"]: # Gets all the default API values
            if d["key"] == "date_slider" : 
                defaults_api[d["key"]] = d["default"]
            else : 
                defaults_api[d["key"]] = d["default"]["value"]

        for k, v in defaults_api.items():
            key = self._params[k][0] if k in self._params else v
            if ( key in table_data ) and ( key in chart_data ):
                table_data = table_data[key]
                chart_data = chart_data[key]
            else:
                tbl_data = {}
                chart_data = {}
                break
        
        self.extract_lang(lang)

        table = {}
        table["columns"] = table_cols[lang]
        table["data"] = table_data

        res = {}
        res["chart_data"] = chart_data
        res["table_data"] = table
        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res
        
        return self._data

    '''
    Check if a chart has a date range element
    '''
    def chart_has_date(self, api_data) :
        if 'has_date' in api_data :
            return api_data['has_date']

        return None
    
    '''
    Sets default if key isn't in request parameters
    '''
    def default_param_value(self, key, default, params) :
        if key in params : 
            return params[key][0]
        
        return default
    
    '''
    Extract languages in other parts of the meta
    '''
    def extract_lang(self, lang) :
        temp = self._data["metadata"]["dataset_desc"][lang]
        self._data["metadata"]["dataset_desc"] = temp

        temp = self._data["explanation"][lang]
        self._data["explanation"] = temp

        for j in ["in_dataset", "out_dataset"] :
            if j in self._data["metadata"] : 
                for i in self._data["metadata"][j] :
                    desc = i[f"desc_{lang}"]
                    title = i[f"title_{lang}"]
                    i["desc"] = desc
                    i["title"] = title
                    [ i.pop(k) for k in ["desc_en", "desc_bm", "title_en", "title_bm"]]

    '''
    Extract languages from intro key
    '''
    def extract_lang_intro(self, lang, intro) :
        lang_info = intro[lang]
        intro.pop("en")
        intro.pop("bm")
        intro.update(lang_info)
        return intro


    def extract_lang_table_view(self, lang, keys, data) :        
        for d in data : 
            for k in keys : 
                if f"{k}_{lang}" in d :
                    d[k] = d[f"{k}_{lang}"]
                for l in ["en", "bm"] :
                    if f"{k}_{l}" in d : 
                        d.pop(f"{k}_{l}")
        
        return data

