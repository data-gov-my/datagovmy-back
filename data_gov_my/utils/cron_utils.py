import os
from os import listdir
from os.path import isfile, join
from data_gov_my.models import MetaJson, DashboardJson
from data_gov_my.utils import triggers
from data_gov_my.utils import data_utils
from data_gov_my.utils import common
from data_gov_my.catalog_utils import catalog_builder

from data_gov_my.api_handling import cache_search

from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from data_gov_my.models import CatalogJson, MetaJson, DashboardJson
from django.apps import apps

import requests
import zipfile
import json
import shutil

"""
Creates a directory
"""


def create_directory(dir_name):
    try:
        os.mkdir(os.path.join(os.getcwd(), dir_name))
    except OSError as error:
        print("Directory already exists, no need to create")


"""
Fetches entire content from a git repo
"""


def fetch_from_git(zip_name, git_url):
    file_name = os.path.join(os.getcwd(), zip_name)
    headers = {
        # "Authorization": f"token {git_token}",
        "Accept": "application/vnd.github.v3.raw",
    }

    res = {}
    res["file_name"] = file_name
    res["data"] = requests.get(git_url, headers=headers)
    res["resp_code"] = res["data"].status_code
    return res


"""
Writes content as binary
"""


def write_as_binary(file_name, data):
    try:
        with open(file_name, "wb") as f:
            f.write(data.content)
    except:
        triggers.send_telegram("!! FILE ISSUES WRITING TO BINARY !!")


"""
Extracts zip file into desired directory
"""


def extract_zip(file_name, dir_name):
    try:
        with zipfile.ZipFile(file_name, "r") as zip_ref:
            zip_ref.extractall(os.path.join(os.getcwd(), dir_name))
    except:
        triggers.send_telegram("!! ZIP FILE EXTRACTION ISSUE !!")


"""
Performs data operations,
such as update or rebuild
"""


def data_operation(operation, op_method):
    dir_name = "DATAGOVMY_SRC"
    zip_name = "repo.zip"
    git_url = os.getenv("GITHUB_URL", "-")
    # git_token = os.getenv("GITHUB_TOKEN", "-")

    triggers.send_telegram("--- PERFORMING " + op_method + " " + operation + " ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url)
    if "resp_code" in res and res["resp_code"] == 200:
        write_as_binary(res["file_name"], res["data"])
        extract_zip(res["file_name"], dir_name)
        data_utils.rebuild_dashboard_meta(operation, op_method)
        data_utils.rebuild_dashboard_charts(operation, op_method)
    else:
        triggers.send_telegram("FAILED TO GET SOURCE DATA")


def get_latest_info_git(type, commit_id):
    sha_ext = os.getenv("GITHUB_SHA_URL", "-")
    url = "https://api.github.com/repos/data-gov-my/datagovmy-meta/commits/" + sha_ext
    headers_accept = "application/vnd.github.VERSION.sha"

    # git_token = os.getenv("GITHUB_TOKEN", "-")

    if type == "COMMIT":
        url = url.replace(sha_ext, "")
        url += commit_id
        headers_accept = "application/vnd.github+json"

    res = requests.get(
        url, headers={"Accept": headers_accept}
    )

    if res.status_code == 200:
        return str(res.content, "UTF-8")
    else:
        triggers.send_telegram("!!! FAILED TO GET GITHUB " + type + " !!!")


def selective_update():
    # Delete all file src
    # os.remove("repo.zip")
    # shutil.rmtree("DATAGOVMY_SRC/")
    remove_src_folders()

    dir_name = "DATAGOVMY_SRC"
    zip_name = "repo.zip"
    git_url = os.getenv("GITHUB_URL", "-")
    # git_token = os.getenv("GITHUB_TOKEN", "-")

    triggers.send_telegram("--- PERFORMING SELECTIVE UPDATE ---")

    create_directory(dir_name)
    res = fetch_from_git(zip_name, git_url)
    if "resp_code" in res and res["resp_code"] == 200:
        write_as_binary(res["file_name"], res["data"])
        extract_zip(res["file_name"], dir_name)

        latest_sha = get_latest_info_git("SHA", "")
        data = json.loads(get_latest_info_git("COMMIT", latest_sha))
        changed_files = [f["filename"] for f in data["files"]]
        filtered_changes = filter_changed_files(changed_files)

        remove_deleted_files()

        if filtered_changes["dashboards"]:
            fin_files = [x.replace(".json", "") for x in filtered_changes["dashboards"]]
            file_list = ",".join(fin_files)

            triggers.send_telegram("Updating : " + file_list)

            operation = "UPDATE " + file_list
            data_utils.rebuild_dashboard_meta(operation, "AUTO")
            validate_info = data_utils.rebuild_dashboard_charts(operation, "AUTO")

            dashboards_validate = validate_info["dashboard_list"]
            failed_dashboards = validate_info["failed_dashboards"]

            # Validate each dashboard
            dashboards_validate_status = []

            for dbd in dashboards_validate:
                if dbd not in failed_dashboards:
                    if revalidate_frontend(dbd) == 200:
                        dashboards_validate_status.append(
                            {"status": "✅", "variable": dbd}
                        )
                    else:
                        dashboards_validate_status.append(
                            {"status": "❌", "variable": dbd}
                        )
                else:
                    dashboards_validate_status.append({"status": "❌", "variable": dbd})

            if dashboards_validate_status:
                revalidation_results = triggers.format_status_message(
                    dashboards_validate_status, "-- DASHBOARD REVALIDATION STATUS --"
                )
                triggers.send_telegram(revalidation_results)

        if filtered_changes["catalog"]:
            fin_files = [x.replace(".json", "") for x in filtered_changes["catalog"]]
            file_list = ",".join(fin_files)
            operation = "UPDATE " + file_list
            catalog_builder.catalog_update(operation, "AUTO")

            # Update Cache Here
            source_filters_cache()
            catalog_list = list(
                CatalogJson.objects.all().values(
                    "id",
                    "catalog_name",
                    "catalog_category",
                    "catalog_category_name",
                    "catalog_subcategory_name",
                )
            )
            cache.set("catalog_list", catalog_list)
            # cache_search.set_filter_cache()
        
        if filtered_changes["i18n"]:
            fin_files = [x.replace(".json", "") for x in filtered_changes["i18n"]]
            file_list = ",".join(fin_files)
            filenames = filtered_changes["i18n"]
            triggers.send_telegram("Updating : " + file_list)
            operation = "UPDATE " + file_list

            data_utils.rebuild_i18n(operation, "AUTO")

    else:
        triggers.send_telegram("FAILED TO GET SOURCE DATA")


"""
Filters the changed files for dashboards, catalog and i18n data
"""


def filter_changed_files(file_list):
    changed_files = {"dashboards": [], "catalog": [], "i18n": []}

    for f in file_list:
        f_path = "DATAGOVMY_SRC/" + os.getenv("GITHUB_DIR", "-") + "/" + f
        f_info = f.split("/")
        if len(f_info) > 1 and f_info[0] in changed_files and os.path.exists(f_path):
            changed_files[f_info[0]].append(os.path.join(*f_info[1:]))

    return changed_files


"""
Remove deleted files
"""


def remove_deleted_files():
    for k, v in common.REFRESH_VARIABLES.items():
        model_name = apps.get_model("data_gov_my", k)
        distinct_db = [
            m[v["column_name"]]
            for m in model_name.objects.values(v["column_name"]).distinct()
        ]
        DIR = os.path.join(
            os.getcwd(), "DATAGOVMY_SRC/" + os.getenv("GITHUB_DIR", "-") + v["directory"]
        )
        distinct_dir = [
            f.replace(".json", "") for f in listdir(DIR) if isfile(join(DIR, f))
        ]
        diff = list(set(distinct_db) - set(distinct_dir))

        if diff:
            # Remove the deleted datasets
            query = {v["column_name"] + "__in": diff}
            model_name.objects.filter(**query).delete()

    # Update the cache
    source_filters_cache()
    catalog_list = list(
        CatalogJson.objects.all().values(
            "id",
            "catalog_name",
            "catalog_category",
            "catalog_category_name",
            "catalog_subcategory_name",
        )
    )
    cache.set("catalog_list", catalog_list)


"""
Revalidate Frontend
"""


def revalidate_frontend(dashboard):
    if dashboard not in common.FRONTEND_ENDPOINTS:
        return -1

    endpoint = common.FRONTEND_ENDPOINTS[dashboard]
    url = os.getenv("FRONTEND_URL", "-")
    fe_auth = os.getenv("FRONTEND_REBUILD_AUTH", "-")

    headers = {"Authorization": fe_auth}
    body = {"route": endpoint}

    # Comment out temporarily
    # response = requests.post(url, headers=headers, data=body)

    # return response.status_code
    return 200


"""
Set Source Filters Cache
"""


def source_filters_cache():
    filter_sources_distinct = CatalogJson.objects.values("data_source").distinct()
    source_filters = set()

    for x in filter_sources_distinct:
        if "|" in x["data_source"]:
            sources = x["data_source"].split(" | ")
            for s in sources:
                source_filters.add(s)
        else:
            source_filters.add(x["data_source"])

    cache.set("source_filters", list(source_filters))

    return list(source_filters)


"""
REMOVE SRC FOLDERS
"""


def remove_src_folders():
    if os.path.exists("DATAGOVMY_SRC") and os.path.isdir("DATAGOVMY_SRC"):
        shutil.rmtree("DATAGOVMY_SRC")
    if os.path.exists("repo.zip"):
        os.remove("repo.zip")
