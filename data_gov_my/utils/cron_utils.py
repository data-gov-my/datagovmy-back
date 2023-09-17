import logging
import os
import shutil
import zipfile
import boto3
import json
from os import listdir
from os.path import isfile, join

import requests
from django.apps import apps
from django.core.cache import cache

from data_gov_my.models import CatalogJson
from data_gov_my.utils import common, triggers


def create_directory(dir_name):
    """
    Creates a new directory if not yet exist.
    """
    try:
        os.mkdir(os.path.join(os.getcwd(), dir_name))
    except OSError as error:
        print("Directory already exists, no need to create")


def fetch_from_git(zip_name, git_url):
    """
    Fetches entire content from a git repo
    """
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


def write_as_binary(file_name, data):
    """
    Writes input data as binary to file_name.
    """
    try:
        with open(file_name, "wb") as f:
            f.write(data.content)
    except:
        triggers.send_telegram("!! FILE ISSUES WRITING TO BINARY !!")


def extract_zip(file_name, dir_name):
    """
    Extracts zip file into desired directory
    """
    try:
        with zipfile.ZipFile(file_name, "r") as zip_ref:
            zip_ref.extractall(os.path.join(os.getcwd(), dir_name))
    except:
        triggers.send_telegram("!! ZIP FILE EXTRACTION ISSUE !!")


def get_latest_info_git(type, commit_id):
    """
    Get the latest github commit information.
    """
    sha_ext = os.getenv("GITHUB_SHA_URL", "-")
    url = "https://api.github.com/repos/data-gov-my/datagovmy-meta/commits/" + sha_ext
    headers_accept = "application/vnd.github.VERSION.sha"

    # git_token = os.getenv("GITHUB_TOKEN", "-")

    if type == "COMMIT":
        url = url.replace(sha_ext, "")
        url += commit_id
        headers_accept = "application/vnd.github+json"

    res = requests.get(url, headers={"Accept": headers_accept})

    if res.status_code == 200:
        return str(res.content, "UTF-8")
    else:
        triggers.send_telegram("!!! FAILED TO GET GITHUB " + type + " !!!")


def remove_deleted_files():
    """
    Remove deleted files.
    """
    for k, v in common.REFRESH_VARIABLES.items():
        model_name = apps.get_model("data_gov_my", k)
        distinct_db = [
            m[v["column_name"]]
            for m in model_name.objects.values(v["column_name"]).distinct()
        ]
        DIR = os.path.join(
            os.getcwd(),
            "DATAGOVMY_SRC/" + os.getenv("GITHUB_DIR", "-") + v["directory"],
        )
        distinct_dir = [
            f.replace(".json", "") for f in listdir(DIR) if isfile(join(DIR, f))
        ]
        diff = list(set(distinct_db) - set(distinct_dir))

        if diff:
            # Remove the deleted datasets
            query = {v["column_name"] + "__in": diff}
            del_int, deleted = model_name.objects.filter(**query).delete()
            logging.warning(f"Deleted removed files: {deleted}")

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


def get_fe_url_by_site(site):
    """
    Returns the URL and authorization header for frontend revalidation.
    """
    if site == "datagovmy":
        return os.getenv("DATAGOVMY_FRONTEND_URL"), os.getenv(
            "DATAGOVMY_FRONTEND_REBUILD_AUTH"
        )
    elif site == "opendosm":
        return os.getenv("OPENDOSM_FRONTEND_URL"), os.getenv(
            "OPENDOSM_FRONTEND_REBUILD_AUTH"
        )
    elif site == "kkmnow":
        return os.getenv("KKMNOW_FRONTEND_URL"), os.getenv(
            "KKMNOW_FRONTEND_REBUILD_AUTH"
        )
    else:
        raise ValueError(
            f"{site} is not a valid site for revalidation! (Currently supports datagovmy, opendosm, kkmnow only.)"
        )


def revalidate_frontend(routes=[], site="datagovmy"):
    """
    Revalidate frontend pages based on input routes.
    """
    if isinstance(routes, str):
        routes = routes.split(",")
    endpoint = ",".join(route for route in routes if isinstance(route, str))

    url, fe_auth = get_fe_url_by_site(site)

    headers = {
        "Authorization": fe_auth,
        "Content-Type": "application/x-www-form-urlencoded",
    }
    payload = f"route={endpoint}"

    response = requests.post(url, headers=headers, data=payload)
    return response


def source_filters_cache():
    """
    Set Source Filters Cache.
    """
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


def remove_src_folders():
    """
    Remove locally cloned github source folders (`DATAGOVMY_SRC/`).
    """
    if os.path.exists("DATAGOVMY_SRC") and os.path.isdir("DATAGOVMY_SRC"):
        shutil.rmtree("DATAGOVMY_SRC")
    if os.path.exists("repo.zip"):
        os.remove("repo.zip")


def upload_s3(data, bucket, key):
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("S3_KEY_ID"),
            aws_secret_access_key=os.getenv("S3_KEY_SECRET"),
        )

        json_str = json.dumps(data)

        s3.put_object(Body=json_str, Bucket=bucket, Key=key)
        return True
    except Exception as e:
        return False
