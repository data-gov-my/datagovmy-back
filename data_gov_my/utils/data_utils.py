from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from data_gov_my.models import MetaJson, DashboardJson
from data_gov_my.utils import dashboard_builder
from data_gov_my.utils import triggers
from data_gov_my.utils import common

import os
from os import listdir
from os.path import isfile, join
import json

"""
Operations to rebuild all meta, from each dashboard

HOW IT WORKS :
    - Get each file within the META_JSON directory
    - Fetch data within file
    - If META doesn't exist, insert, else, update

"""


def rebuild_dashboard_meta(operation, op_method):
    opr_data = get_operation_files(operation)
    operation = opr_data["operation"]
    meta_files = opr_data["files"]

    META_DIR = os.path.join(
        os.getcwd(), "AKSARA_SRC/" + os.getenv("GITHUB_DIR", "-") + "/dashboards/"
    )

    if operation == "REBUILD":
        MetaJson.objects.all().delete()

    if not meta_files:
        meta_files = [f for f in listdir(META_DIR) if isfile(join(META_DIR, f))]
    else:
        meta_files = [f + ".json" for f in meta_files]

    failed_builds = []

    for meta in meta_files:
        try:
            f_meta = META_DIR + meta
            f = open(f_meta)
            data = json.load(f)
            dbd_name = meta.replace(".json", "")

            updated_values = {"dashboard_meta": data}
            obj, created = MetaJson.objects.update_or_create(
                dashboard_name=dbd_name, defaults=updated_values
            )
            obj.save()

            cache.set("META_" + dbd_name, data)
        except Exception as e:
            failed_obj = {}
            failed_obj["DASHBOARD_NAME"] = dbd_name
            failed_obj["ERROR"] = e
            failed_builds.append(failed_obj)

    if len(failed_builds) > 0:
        err_message = triggers.format_multi_line(failed_builds, "--- FAILED META ---")
        triggers.send_telegram(err_message)
    else:
        print("Meta Built successfully.")
        triggers.send_telegram("META Built Successfully.")


"""
Operations to rebuild all charts, from each dashboard.

HOW IT WORKS :
    - Check what the operation is, if REBUILD, clear all existing chart data
    - Retrieve all meta jsons from db
    - Build all charts, according to charts within meta json

"""


def rebuild_dashboard_charts(operation, op_method):
    opr_data = get_operation_files(operation)
    operation = opr_data["operation"]
    meta_files = opr_data["files"]
    meta_json_list = []

    dashboard_list = set()
    failed_notify = []

    if operation == "REBUILD":
        DashboardJson.objects.all().delete()

    if meta_files:
        meta_json_list = MetaJson.objects.filter(dashboard_name__in=meta_files).values()
    else:
        meta_json_list = MetaJson.objects.values()

    failed_builds = []

    for meta in meta_json_list:
        dbd_meta = meta["dashboard_meta"]
        dbd_name = meta["dashboard_name"]
        chart_list = dbd_meta["charts"]

        dashboard_list.add(dbd_name)

        for k in chart_list.keys():
            chart_name = k
            chart_type = chart_list[k]["chart_type"]
            c_data = {}
            c_data["variables"] = chart_list[k]["variables"]
            c_data["input"] = chart_list[k]["chart_source"]
            api_type = chart_list[k]["api_type"]
            try:
                res = {}
                res["data"] = dashboard_builder.build_chart(
                    chart_list[k]["chart_type"], c_data
                )
                if len(res["data"]) > 0:  # If the dict isnt empty

                    if "data_as_of" in chart_list[k]:
                        res["data_as_of"] = chart_list[k]["data_as_of"]

                    updated_values = {
                        "chart_type": chart_type,
                        "api_type": api_type,
                        "chart_data": res,
                    }
                    obj, created = DashboardJson.objects.update_or_create(
                        dashboard_name=dbd_name, chart_name=k, defaults=updated_values
                    )
                    obj.save()
                    cache.set(dbd_name + "_" + k, res)
            except Exception as e:
                failed_notify.append(meta)
                failed_obj = {}
                failed_obj["CHART_NAME"] = chart_name
                failed_obj["DASHBOARD"] = dbd_name
                failed_obj["ERROR"] = str(e)
                failed_builds.append(failed_obj)

    if len(failed_builds) > 0:
        err_message = triggers.format_multi_line(failed_builds, "--- FAILED CHARTS ---")
        triggers.send_telegram(err_message)
    else:
        print("Chart data built successfully")
        triggers.send_telegram("Chart Data Built Successfully.")

    validate_info = {}
    validate_info["dashboard_list"] = dashboard_list
    validate_info["failed_dashboards"] = failed_notify

    return validate_info


"""
Operations to fetch the latest data update
"""


def get_latest_data_update(arr, data):
    for a in arr:
        if a in data:
            data = data[a]
        else:
            data = None
            break

    return data


def get_operation_files(operation):
    opr = operation.split(" ")
    chosen_opr = opr[0]
    files = []

    if len(opr) > 1:
        files = opr[1].split(",")

    return {"operation": chosen_opr, "files": files}
