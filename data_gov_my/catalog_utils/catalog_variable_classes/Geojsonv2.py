from data_gov_my.catalog_utils.catalog_variable_classes.Generalv2 import (
    GeneralChartsUtil,
)

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

    def __init__(self, full_meta, file_data, cur_data, all_variable_data, file_src):
        GeneralChartsUtil.__init__(
            self, full_meta, file_data, cur_data, all_variable_data, file_src
        )

        # self.validate_meta_json()

        self.c_color = self.chart["chart_variables"]["color"]
        self.c_file_json = self.chart["chart_variables"]["file_json"]

        self.rebuild_downloads()

        self.chart_name = {}
        self.chart_name["en"] = self.cur_data["title_en"]
        self.chart_name["bm"] = self.cur_data["title_bm"]

        self.metadata = self.rebuild_metadata()
        self.api = self.build_api()

        self.chart_details["chart"] = {}
        self.db_input["catalog_data"] = self.build_catalog_data_info()

    """
    Replace link for downloads
    """

    def rebuild_downloads(self):
        self.downloads = {"link_geojson": self.file_data["link_geojson"]}

    """
    Rebuild metadata accordingly
    """

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

    """
    Builds the API details
    """

    def build_api(self):
        res = {}
        res["API"] = {}
        res["API"]["chart_type"] = self.chart["chart_type"]
        res["API"]["color"] = self.c_color
        res["API"]["file_json"] = self.c_file_json

        return res["API"]
