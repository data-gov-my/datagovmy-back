from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from data_gov_my.catalog_utils import general_helper as gh
from data_gov_my.catalog_utils.catalog_variable_classes import Timeseries as tm
from data_gov_my.catalog_utils.catalog_variable_classes import Choropleth as ch
from data_gov_my.catalog_utils.catalog_variable_classes import Table as tb
from data_gov_my.catalog_utils.catalog_variable_classes import Geojson as gj
from data_gov_my.catalog_utils.catalog_variable_classes import Bar as bar
from data_gov_my.catalog_utils.catalog_variable_classes import Heatmap as hm
from data_gov_my.catalog_utils.catalog_variable_classes import Pyramid as py


from data_gov_my.utils import cron_utils, data_utils, triggers
from data_gov_my.models import CatalogJson

import os
from os import listdir
from os.path import isfile, join
import pathlib


def catalog_update(operation, op_method):
    opr_data = data_utils.get_operation_files(operation)
    operation = opr_data["operation"]
    meta_files = opr_data["files"]

    META_DIR = os.path.join(
        os.getcwd(), "DATAGOVMY_SRC/" + os.getenv("GITHUB_DIR", "-") + "/catalog/"
    )

    if operation == "REBUILD":
        CatalogJson.objects.all().delete()

    if not meta_files:
        meta_files = [f for f in listdir(META_DIR) if isfile(join(META_DIR, f))]
    else:
        meta_files = [f + ".json" for f in meta_files]

    for meta in meta_files:
        try:
            FILE_META = os.path.join(
                os.getcwd(),
                "DATAGOVMY_SRC/" + os.getenv("GITHUB_DIR", "-") + "/catalog/" + meta,
            )

            if pathlib.Path(meta).suffix == ".json":
                data = gh.read_json(FILE_META)
                file_data = data["file"]
                all_variable_data = data["file"]["variables"]
                catalog_data = data["catalog_data"]
                full_meta = data
                file_src = meta.replace(".json", "")

                failed_builds = []
                all_builds = []

                for cur_data in catalog_data:
                    try:
                        chart_type = cur_data["chart"]["chart_type"]
                        obj = []
                        variable_data = {}
                        # variable_data = all_variable_data[ cur_data['id'] - 1 ]

                        for var in all_variable_data:
                            if chart_type in ["TABLE", "GEOJSON", "PYRAMID"]:
                                if var["id"] == 0:
                                    variable_data = var
                                    break
                            else:
                                if cur_data["id"] == var["id"]:
                                    variable_data = var
                                    break

                        if chart_type == "TIMESERIES":
                            obj = tm.Timeseries(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "CHOROPLETH":
                            obj = ch.Choropleth(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "TABLE":
                            obj = tb.Table(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "GEOJSON":
                            obj = gj.Geojson(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "HBAR" or chart_type == "BAR":
                            obj = bar.Bar(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "HEATMAP":
                            obj = hm.Heatmap(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )
                        elif chart_type == "PYRAMID":
                            obj = py.Pyramid(
                                full_meta,
                                file_data,
                                cur_data,
                                variable_data,
                                all_variable_data,
                                file_src,
                            )

                        db_input = obj.db_input
                        unique_id = obj.unique_id

                        db_obj, created = CatalogJson.objects.update_or_create(
                            id=unique_id, defaults=db_input
                        )

                        cache.set(unique_id, db_input["catalog_data"])
                        all_builds.append(
                            {"status": "✅", "variable": variable_data["name"]}
                        )
                    except Exception as e:
                        all_builds.append(
                            {"status": "❌", "variable": variable_data["name"]}
                        )
                        err_obj = {
                            "variable name": variable_data["name"],
                            "exception": str(e),
                        }
                        failed_builds.append(err_obj)

                if len(all_builds) > 0:
                    results = triggers.format_status_message(
                        all_builds, "Results for " + file_src
                    )
                    triggers.send_telegram(results)

                if len(failed_builds) > 0:
                    results = triggers.format_multi_line(
                        failed_builds, "❌ Failed Builds for " + file_src + " ❌"
                    )
                    triggers.send_telegram(results)

        except Exception as e:
            triggers.send_telegram(str(e))


def catalog_operation(operation, op_method):
    try:
        dir_name = "DATAGOVMY_SRC"
        zip_name = "repo.zip"
        git_url = os.getenv("GITHUB_URL", "-")
        # git_token = os.getenv("GITHUB_TOKEN", "-")

        cron_utils.create_directory(dir_name)
        res = cron_utils.fetch_from_git(zip_name, git_url)

        if "resp_code" in res and res["resp_code"] == 200:
            triggers.send_telegram(
                "------- PERFORMING " + op_method + " DATA CATALOG UPDATE -------"
            )
            cron_utils.write_as_binary(res["file_name"], res["data"])
            cron_utils.extract_zip(res["file_name"], dir_name)
            catalog_update(operation, op_method)
    except Exception as e:
        triggers.send_telegram(str(e))
