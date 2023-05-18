from django.http import JsonResponse
from rest_framework import status, viewsets, generics
from rest_framework.decorators import api_view, permission_classes
from rest_framework import permissions
from rest_framework.response import Response
from rest_framework.views import APIView
from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from functools import reduce
from django.db.models import Q
from django.db import models
from django.db import connection
from django.apps import apps
from django.db.models.base import ModelBase
from django.contrib.postgres.search import TrigramWordSimilarity
from django.contrib.postgres.search import SearchVector
from django.contrib.postgres.search import SearchHeadline
from data_gov_my.forms import ModsDataForm
from data_gov_my.serializers import ModsDataSerializer, i18nSerializer
from django.shortcuts import get_object_or_404, get_list_or_404
from rest_framework.exceptions import ParseError
from post_office.models import EmailTemplate
from post_office import mail

from data_gov_my.utils import cron_utils, triggers
from data_gov_my.models import (
    MetaJson,
    DashboardJson,
    CatalogJson,
    ModsData,
    NameDashboard_FirstName,
    NameDashboard_LastName,
    i18nJson,
)
from data_gov_my.api_handling import handle, cache_search
from data_gov_my.explorers import class_list as exp_class
from data_gov_my.catalog_utils.catalog_variable_classes import (
    CatalogueDataHandler as cdh,
)

from threading import Thread

import json
import os
import environ
import pandas as pd
import time
import numpy as np
import sys

env = environ.Env()
environ.Env.read_env()

"""
Endpoint for all single charts
"""


class CHART(APIView):
    def get(self, request, format=None):
        param_list = dict(request.GET)
        params_req = ["dashboard", "chart_name"]

        if all(p in param_list for p in params_req):
            dbd_name = param_list["dashboard"][0]
            chart_name = param_list["chart_name"][0]

            meta = MetaJson.objects.filter(dashboard_name=dbd_name).values(
                "dashboard_meta"
            )[0]["dashboard_meta"]
            api_params = meta["charts"][chart_name]["api_params"]
            chart_type = meta["charts"][chart_name]["chart_type"]
            api_type = meta["charts"][chart_name]["api_type"]
            chart_variables = meta["charts"][chart_name]["variables"]

            chart_data = DashboardJson.objects.filter(
                dashboard_name=dbd_name, chart_name=chart_name
            ).values("chart_data")[0]["chart_data"]

            data_as_of = chart_data["data_as_of"]
            chart_data = chart_data["data"]

            #  TEMP FIX
            temp = {}
            if chart_type == "timeseries_shared":
                const_keys = list(chart_variables["constant"].keys())
                for k in const_keys:
                    temp[k] = chart_data[k]

            for api in api_params:
                if api in param_list:
                    chart_data = chart_data[param_list[api][0]]
                else:
                    return JsonResponse({}, safe=False)

            if temp:
                chart_data.update(temp)

            overall_data = {}
            overall_data["data"] = chart_data
            overall_data["data_as_of"] = data_as_of

        return JsonResponse(overall_data, safe=False)


class UPDATE(APIView):
    def post(self, request, format=None):
        if is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            thread = Thread(target=cron_utils.selective_update)
            thread.start()
            return Response(status=status.HTTP_200_OK)
        return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)


class DASHBOARD(APIView):
    def get(self, request, format=None):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        param_list = dict(request.GET)
        params_req = ["dashboard"]

        if all(p in param_list for p in params_req):
            res = handle_request(param_list)
            res = handle.dashboard_additional_handling(param_list, res)
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)


class DATA_VARIABLE(APIView):
    def get(self, request, format=None):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        param_list = dict(request.GET)
        params_req = ["id"]

        if all(p in param_list for p in params_req):
            res = data_variable_handler(param_list)
            if not res:
                return JsonResponse(
                    {"status": 404, "message": "Catalogue data not found."}, status=404
                )
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)


class DATA_CATALOG(APIView):
    def get(self, request, format=None):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        param_list = dict(request.GET)
        filters = get_filters_applied(param_list)
        info = ""

        if len(filters) > 0:
            # search_cache = cache.get("search_cache")
            # filter_list = cache_search.filter_options(param_list)
            # info = cache_search.filter_cache(filter_list, search_cache)

            info = CatalogJson.objects.filter(filters).values(
                "id",
                "catalog_name",
                "catalog_category",
                "catalog_category_name",
                "catalog_subcategory_name",
            )
        else:
            catalog_list = cache.get("catalog_list")

            if catalog_list:
                info = catalog_list
            else:
                info = list(
                    CatalogJson.objects.all().values(
                        "id",
                        "catalog_name",
                        "catalog_category",
                        "catalog_category_name",
                        "catalog_subcategory_name",
                    )
                )
                cache.set("catalog_list", info)

        res = {}
        res["total_all"] = len(info)
        if cache.get("source_filters"):
            res["source_filters"] = cache.get("source_filters")
        else:
            source_filters = cron_utils.source_filters_cache()
            res["source_filters"] = source_filters
            cache.set("source_filters", source_filters)
        # res["source_filters"] = cache.get('source_filters') if cache.get('source_filters') else cron_utils.source_filters_cache()

        res["dataset"] = {}

        lang = request.query_params.get("lang", "en")
        lang_mapping = {"en": 0, "bm": 1}

        if lang not in lang_mapping:
            lang = "en"

        for item in info:
            category = item["catalog_category_name"].split(" | ")[lang_mapping[lang]]
            sub_category = item["catalog_subcategory_name"].split(" | ")[
                lang_mapping[lang]
            ]

            obj = {}
            obj["catalog_name"] = item["catalog_name"].split(" | ")[lang_mapping[lang]]
            obj["id"] = item["id"]

            if category not in res["dataset"]:
                res["dataset"][category] = {}
                res["dataset"][category][sub_category] = [obj]
            else:
                if sub_category in res["dataset"][category]:
                    res["dataset"][category][sub_category].append(obj)
                else:
                    res["dataset"][category][sub_category] = [obj]

        return JsonResponse(res, safe=False)


class EXPLORER(APIView):
    def get(self, request, format=None):
        params = dict(request.GET)
        if (
            "explorer" in params
            and params["explorer"][0] in exp_class.EXPLORERS_CLASS_LIST
        ):
            obj = exp_class.EXPLORERS_CLASS_LIST[params["explorer"][0]]()
            return obj.handle_api(params)

        return JsonResponse({"status": 400, "message": "Bad Request"}, status=400)

    # TODO: Protect this, or remove once meta is created
    def post(self, request, format=None):
        params = dict(request.POST)

        if (
            "explorer" in params
            and params["explorer"][0] in exp_class.EXPLORERS_CLASS_LIST
        ):
            obj = exp_class.EXPLORERS_CLASS_LIST[params["explorer"][0]]()
            to_rebuild = "rebuild" in params
            r_table = params["table"][0] if "table" in params else ""
            obj.populate_db(table=r_table, rebuild=to_rebuild)

        return JsonResponse({"status": 200, "message": "Table Populated."}, status=200)


class DROPDOWN(APIView):
    def get(self, request, format=None):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        param_list = dict(request.GET)
        params_req = ["dashboard"]

        if all(p in param_list for p in params_req):
            res = handle_request(param_list, False)
            dropdown_lst = res["query_values"]["data"]["data"]
            info = {"total": len(dropdown_lst)}
            filtered_res = dropdown_lst
            if query := param_list.get("query"):
                query = query[0].lower()
                if filters := param_list.get("filters"):
                    filters = filters[0].split(",")
                else:
                    # by default take all columns
                    filters = filtered_res[0].keys()
                for column in filters:
                    if column not in filtered_res[0]:
                        return JsonResponse(
                            {"error": f"{column} is not a valid filter column."},
                            status=400,
                        )
                filtered_res = [
                    d
                    for d in filtered_res
                    if any(query.lower() in d[column].lower() for column in filters)
                ]

            if limit := param_list.get("limit"):
                limit = int(limit[0])
                filtered_res = filtered_res[:limit]
                info["limit"] = limit

            res = {"info": info, "data": filtered_res}
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)


class I18N(APIView):
    def get(self, request, *args, **kwargs):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        if {"filename", "lang"} <= request.query_params.keys():  # return all
            queryset = get_object_or_404(
                i18nJson,
                filename=request.query_params["filename"],
                language=request.query_params["lang"],
            )
            serializer = i18nSerializer(queryset)
            res = serializer.data["translation"]
        else:  # return all
            queryset = get_list_or_404(i18nJson)
            serializer = i18nSerializer(queryset, many=True)
            res = {"en-GB": [], "ms-MY": []}
            for file in serializer.data:
                res[file["language"]].append(file["filename"])

        return JsonResponse(res, status=status.HTTP_200_OK)

    def post(self, request, *args, **kwargs):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)
        serializer = i18nSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.validated_data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def patch(self, request, *args, **kwargs):
        if not is_valid_request(request, os.getenv("WORKFLOW_TOKEN")):
            return JsonResponse({"status": 401, "message": "unauthorized"}, status=401)

        if {"filename", "lang"} <= request.query_params.keys():  # return all
            i18n_object = get_object_or_404(
                i18nJson,
                filename=request.query_params["filename"],
                language=request.query_params["lang"],
            )
            serializer = i18nSerializer(
                instance=i18n_object, data=request.data, partial=True
            )
            if serializer.is_valid():
                serializer.save()
                return Response(
                    serializer.validated_data, status=status.HTTP_204_NO_CONTENT
                )
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        return JsonResponse(
            data={
                "detail": "Query parameter filename & lang is required to update i18n object."
            },
            status=status.HTTP_400_BAD_REQUEST,
        )


class MODS(generics.ListAPIView):
    EMAIL_TEMPLATE = EmailTemplate.objects.get_or_create(
        name="mods_form",
        subject="Mods Application | {{ expertise_area }}",
        content="Hi {{ name }}, we have received your request, and will reply to you as soon as we can.",
        html_content="Hi <strong>{{ name }}</strong>, we have received your request, and will reply to you as soon as we can.",
    )
    queryset = ModsData.objects.all()
    serializer_class = ModsDataSerializer

    # TODO: protect access ?
    def post(self, request, *args, **kwargs):
        form = ModsDataForm(request.POST)
        if not form.is_valid():
            return JsonResponse(
                data={"errors": form.errors}, status=status.HTTP_400_BAD_REQUEST
            )

        modsData: ModsData = form.save()
        e = mail.send(
            recipients=modsData.email,
            template="mods_form",
            context={"expertise_area": modsData.expertise_area, "name": modsData.name},
        )

        return JsonResponse(
            data={"message": f"Your request has been received: {e}"},
            status=status.HTTP_200_OK,
        )

    def delete(self, request, *args, **kwargs):
        queryset = ModsData.objects.all()
        if id := request.query_params.get("id", False):
            queryset = ModsData.objects.get(id=id)
        deleted = queryset.delete()
        return JsonResponse(
            data={"message": f"Deleted {deleted[0]} form data."},
            status=status.HTTP_204_NO_CONTENT,
        )

"""
Checks which filters have been applied for the data-catalog
"""


def get_filters_applied(param_list):
    default_params = {
        "period": "",
        "geographic": [],
        "begin": "",
        "end": "",
        "source": [],
        "search": "",
    }

    for k, v in default_params.items():
        if k in param_list:
            if isinstance(v, str):
                default_params[k] = param_list[k][0]
            else:
                default_params[k] = param_list[k][0].split(",")

    # Check if parameters have been set, remove those which have not
    default_params = {k: v for k, v in default_params.items() if v}

    query = Q()

    for k, v in default_params.items():
        if k == "period":
            query &= Q(time_range=v)
        elif k == "geographic":
            for i in v:
                query |= Q(geographic__contains=i)
        elif k == "source":
            for i in v:
                query |= Q(data_source__contains=i)
        elif k == "search":
            query &= Q(catalog_name__icontains=v)
        if k == "begin":
            query &= Q(dataset_begin__lte=v)
        if k == "end":
            query &= Q(dataset_end__gte=v)

    return query


"""
Handles the data-variable queries, by chart applied
"""


def data_variable_chart_handler(data, chart_type, param_list):
    c_handler = cdh.CatalogueDataHandler(chart_type, data, param_list)
    return c_handler.get_results()


"""
General handler for data-variables
"""


def data_variable_handler(param_list):
    var_id = param_list["id"][0]
    info = cache.get(var_id)

    if not info:
        info = CatalogJson.objects.filter(id=var_id).values("catalog_data")
        if len(info) == 0:  # If catalogue doesn't exist
            return {}
        info = info[0]["catalog_data"]
        cache.set(var_id, info)

    chart_type = info["API"]["chart_type"]
    info = data_variable_chart_handler(info, chart_type, param_list)

    if len(info) == 0:  # If catalogues with the filter isn't found
        return {}
    return info


"""
Handles request for dashboards
"""


def handle_request(param_list, isDashboard=True):
    dbd_name = str(param_list["dashboard"][0])
    dbd_info = cache.get("META_" + dbd_name)

    if not dbd_info:
        dbd_info = MetaJson.objects.filter(dashboard_name=dbd_name).values(
            "dashboard_meta"
        )

    params_req = []

    if len(dbd_info) > 0:
        dbd_info = (
            dbd_info if isinstance(dbd_info, dict) else dbd_info[0]["dashboard_meta"]
        )
        params_req = dbd_info["required_params"]

    res = {}
    if all(p in param_list for p in params_req) or not isDashboard:
        data = dbd_info["charts"]

        if len(data) > 0:
            for k, v in data.items():
                api_type = v["api_type"]
                api_params = v["api_params"]
                cur_chart_data = cache.get(dbd_name + "_" + k)

                # dashboard endpoint should ignore this unless the chart name is query_values
                if (isDashboard and k == "query_values") or (
                    not isDashboard and k != "query_values"
                ):
                    continue

                if not cur_chart_data:
                    cur_chart_data = DashboardJson.objects.filter(
                        dashboard_name=dbd_name, chart_name=k
                    ).values("chart_data")[0]["chart_data"]
                    cache.set(dbd_name + "_" + k, cur_chart_data)

                data_as_of = (
                    None
                    if "data_as_of" not in cur_chart_data
                    else cur_chart_data["data_as_of"]
                )

                if api_type == "static":
                    res[k] = {}
                    if data_as_of:
                        res[k]["data_as_of"] = data_as_of
                    res[k]["data"] = cur_chart_data["data"]
                elif api_type == "dynamic":
                    if len(api_params) > 0:
                        cur_chart_data = get_nested_data(
                            api_params, param_list, cur_chart_data["data"]
                        )

                    if len(cur_chart_data) > 0:
                        res[k] = {}
                        if data_as_of:
                            res[k]["data_as_of"] = data_as_of
                        res[k]["data"] = cur_chart_data

    return res


"""
Slices dictionary,
based on keys within dictionary
"""


def get_nested_data(api_params, param_list, data):
    for a in api_params:
        if a in param_list:
            key = (
                param_list[a][0] if "__FIXED__" not in a else a.replace("__FIXED__", "")
            )
            if key in data:
                data = data[key]
            else:
                raise ParseError(
                    detail=f"The {a} '{key}' is invalid. Please use a valid {a}."
                )
        else:
            data = {}
            break

    return data


"""
Checks whether or not,
a request made is valid
"""


def is_valid_request(request, workflow_token):
    if "Authorization" not in request.headers:
        return False

    secret = request.headers.get("Authorization")
    if secret != workflow_token:
        return False

    return True
