from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import (
    GeneralChartsUtil,
)

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge
from data_gov_my.utils import translations as translation


class CatalogueDataHandler:
    # Default mappings for area-types
    _default_mapping = {
        "state": translation.STATE_TRANSLATIONS,
        "parlimen": translation.PARLIMEN_TRANSLATIONS,
        "district": translation.DISTRICT_TRANSLATIONS,
        "dun": translation.DUN_TRANSLATIONS,
        "range": translation.RANGE_TRANSLATIONS,
    }

    """
    Constructor
    """

    def __init__(self, chart_type, data, params):
        self._chart_type = chart_type
        self._data = data
        self._params = params

    """
    Gets the results depending on which chart type
    """

    def get_results(self):
        # Families with same format
        if self._chart_type in [
            "BAR",
            "HBAR",
            "LINE",
            "STACKED_BAR",
            "AREA",
            "TIMESERIES",
            "STACKED_AREA",
            "PYRAMID",
            "CHOROPLETH",
            "GEOCHOROPLETH",
            "GEOPOINT",
            "SCATTER",
            "INTRADAY",
        ]:
            return self.array_value_handler()
        elif self._chart_type in ["HEATTABLE", "TABLE"]:
            return self.table_view_handler()
        elif self._chart_type in ["GEOJSON"]:
            return self.geo_data_handler()

    """
    Handles chart type Geodata
    """

    def geo_data_handler(self):
        lang = self.default_param_value("lang", "en", self._params)

        res = {}
        intro = self._data["chart_details"]["intro"]
        translations = self._data["translations"]

        self.extract_lang(lang)
        self.set_translations(lang, translations, [])  # Assuming no filters

        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res
        return self._data

    """
    Handles charts with a table view, E.g :
    1. Heatmap
    """

    def table_view_handler(self):
        lang = self.default_param_value("lang", "en", self._params)

        intro = self._data["chart_details"]["intro"]
        translations = self._data["translations"]
        chart_data = self._data["chart_details"]["chart"]  # should put as chart_data(?)

        if self._chart_type == "TABLE":
            chart_data = chart_data

        defaults_api = {}  # Creates a default API
        prev_key = ""

        for d in self._data["API"]["filters"]:  # Gets all the default API values
            key = d["key"]
            val = ""
            if key not in ["date_slider", "range"]:
                if prev_key == "":  # The first key
                    val = self._params[key][0] if key in self._params else d["default"]
                else:
                    val = d["options"][defaults_api[prev_key]][0]
                prev_key = key
            else:
                val = self._params[key][0] if key in self._params else d["default"]
            defaults_api[key] = val

        for k, v in defaults_api.items():
            if v in chart_data:
                chart_data = chart_data[v]
            else:
                chart_data = {}
                break

        self.extract_lang(lang)
        self.set_translations(lang, translations, self._data["API"]["filters"])
        self.toggle_api_filters()

        res = {}

        if self._chart_type == "TABLE":
            res["table_data"] = chart_data
        else:
            res["chart_data"] = chart_data

        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res

        return self._data

    """
    This handler supports of type :
    1. Bar : BAR, HBAR, STACKED_BAR
    2. Timeseries : TIMESERIES, AREA, STACKED_AREA
    """

    def array_value_handler(self):
        lang = self.default_param_value("lang", "en", self._params)

        intro = self._data["chart_details"]["intro"]  # Get intro
        translations = self._data["translations"]
        table_data = self._data["chart_details"]["chart"]["table_data"]
        chart_data = self._data["chart_details"]["chart"][
            "chart_data"
        ]  # Get chart data

        defaults_api = {}  # Creates a default API
        prev_key = ""

        for d in self._data["API"]["filters"]:  # Gets all the default API values
            key = d["key"]
            val = ""
            if key not in ["date_slider", "range"]:
                if prev_key == "":  # The first key
                    val = self._params[key][0] if key in self._params else d["default"]
                else:
                    val = d["options"][defaults_api[prev_key]][0]
                prev_key = key
            else:
                val = self._params[key][0] if key in self._params else d["default"]
            defaults_api[key] = val

        for k, v in defaults_api.items():
            if (v in table_data) and (v in chart_data):
                table_data = table_data[v]
                chart_data = chart_data[v]
            else:
                table_data = {}
                chart_data = {}
                break

        self.extract_lang(lang)
        self.set_translations(lang, translations, self._data["API"]["filters"])
        self.toggle_api_filters()

        res = {}
        res["chart_data"] = chart_data
        res["table_data"] = table_data
        res["intro"] = self.extract_lang_intro(lang, intro)

        self._data["chart_details"] = res

        return self._data

    """
    Handles the toggle for API filters
    """

    def toggle_api_filters(self):
        start_idx = 0

        # Checks to see if the first filter is a date_slider
        if len(self._data["API"]["filters"]) > 0:
            first_filter = self._data["API"]["filters"][0]["key"]
            start_idx = 1 if first_filter == "date_slider" else 0

        for idx, d in enumerate(self._data["API"]["filters"]):
            if (idx > start_idx) and (d["key"] != "range"):
                prev_param = self._data["API"]["filters"][
                    idx - 1
                ]  # Take from previous selection
                if prev_param["key"] in self._params:
                    choice = self._params[prev_param["key"]][0]
                    if choice in d["options"]:
                        d["options"] = d["options"][choice]
                        d["default"] = d["options"][0]
                    else:
                        self._data["API"]["filters"].remove(d)
                else:
                    d["options"] = d["options"][prev_param["default"]]

    """
    Check if a chart has a date range element
    """

    def chart_has_date(self, api_data):
        if "has_date" in api_data:
            return api_data["has_date"]

        return None

    """
    Sets default if key isn't in request parameters
    """

    def default_param_value(self, key, default, params):
        if key in params:
            return params[key][0]

        return default

    """
    Extract languages in other parts of the meta
    """

    def extract_lang(self, lang):
        temp = self._data["metadata"]["dataset_desc"][lang]
        self._data["metadata"]["dataset_desc"] = temp

        temp = self._data["explanation"][lang]
        self._data["explanation"] = temp

        for j in ["in_dataset", "out_dataset"]:
            if j in self._data["metadata"]:
                for i in self._data["metadata"][j]:
                    desc = i[f"desc_{lang}"]
                    title = i[f"title_{lang}"]
                    i["desc"] = desc
                    i["title"] = title
                    [i.pop(k) for k in ["desc_en", "desc_bm", "title_en", "title_bm"]]

    """
    Extract languages from intro key
    """

    def extract_lang_intro(self, lang, intro):
        lang_info = intro[lang]
        intro.pop("en")
        intro.pop("bm")
        intro.update(lang_info)
        return intro

    """
    Extracts the language from a table view
    """

    def extract_lang_table_view(self, lang, keys, data):
        for d in data:
            for k in keys:
                if f"{k}_{lang}" in d:
                    d[k] = d[f"{k}_{lang}"]
                for l in ["en", "bm"]:
                    if f"{k}_{l}" in d:
                        d.pop(f"{k}_{l}")

        return data

    """
    Sets the appropriate translations : en / bm
    """

    def set_translations(self, lang, trans_data, api_filters):
        additional_filters = {}

        for f in api_filters:
            if f["key"] in ["state", "parlimen", "district", "range"]:
                additional_filters.update(self._default_mapping[f["key"]])

        trans_data[lang].update(additional_filters)

        if trans_data[lang]:
            self._data["translations"] = trans_data[lang]
        else:
            self._data.pop("translations")
