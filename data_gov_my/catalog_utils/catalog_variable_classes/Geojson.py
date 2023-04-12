from data_gov_my.catalog_utils.catalog_variable_classes.General import GeneralChartsUtil

import pandas as pd
import numpy as np
import json
from dateutil.relativedelta import relativedelta
from mergedeep import merge


class Geojson(GeneralChartsUtil):
    """Geojson Class for Geojson variables"""

    chart_type = "GEOJSON"

    c_color = ""
    c_file_json = ""

    """
    Initiailize the neccessary data for a table chart
    """

    def __init__(self,full_meta,file_data,meta_data,variable_data,all_variable_data,file_src):
        GeneralChartsUtil.__init__(self,full_meta,file_data,meta_data,variable_data,all_variable_data,file_src)

        # self.validate_meta_json()

        self.c_color = self.meta_data["chart"]["chart_variables"]["color"]
        self.c_file_json = self.meta_data["chart"]["chart_variables"]["file_json"]

        self.rebuild_downloads()

        self.chart_name = {
            "en": self.variable_data["title_en"],
            "bm": self.variable_data["title_bm"],
        }

        self.metadata = self.rebuild_metadata()
        self.api = self.build_api()

        self.chart_details["chart"] = self.build_chart()
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Build the Table chart
    """

    def build_chart(self):
        return {}

    def rebuild_downloads(self):
        self.downloads = {"link_geojson": self.file_data["link_geojson"]}

    def rebuild_metadata(self):
        self.metadata.pop("url", None)
        self.metadata["url"] = {"link_geojson": self.file_data["link_geojson"]}

        self.metadata.pop("in_dataset", None)

        refresh_metadata = []

        for i in self.all_variable_data:
            if i["id"] != 0:
                i.pop("unique_id", None)
                refresh_metadata.append(i)

        self.metadata["out_dataset"] = refresh_metadata
        return self.metadata

    def build_api(self):
        res = {}
        res["API"] = {}
        res["API"]["chart_type"] = self.meta_data["chart"]["chart_type"]
        res["API"]["color"] = self.c_color
        res["API"]["file_json"] = self.c_file_json

        return res["API"]

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
            ["frequency", "geographic", "start", "end", "data_source"],
            src,
            self.meta_data["catalog_filters"],
        )
        s = {"str": ["frequency"], "list": ["geographic", "data_source"]}
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
            ["chart_type", "chart_filters"], src, self.meta_data["chart"]
        )
        s = {"str": ["chart_type"], "dict": ["chart_filters"]}
        self.validate_data_type(s, src, self.meta_data["chart"])

        self.validate_field_presence(
            ["FREEZE", "EXCLUDE"], src, self.meta_data["chart"]["chart_filters"]
        )
        s = {"list": ["FREEZE", "EXCLUDE"]}
        self.validate_data_type(s, src, self.meta_data["chart"]["chart_filters"])
