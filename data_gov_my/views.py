import json
import logging
from datetime import datetime
from itertools import groupby
from threading import Thread

import environ
from django.core.cache import cache
from django.db.models import F, Q
from django.http import JsonResponse, QueryDict
from django.shortcuts import get_list_or_404, get_object_or_404
from django.utils.timezone import get_current_timezone
from post_office import mail
from post_office.models import Email
from rest_framework import generics, request, status
from rest_framework.decorators import api_view
from rest_framework.exceptions import ParseError
from rest_framework.filters import SearchFilter
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

import data_gov_my.utils.viewcount_cache as vcc
from data_gov_my.api_handling import handle
from data_gov_my.catalog_utils.catalog_variable_classes import (
    CatalogueDataHandler as cdh,
)
from data_gov_my.explorers import class_list as exp_class
from data_gov_my.models import (
    AuthTable,
    CatalogJson,
    DashboardJson,
    FormData,
    FormTemplate,
    MetaJson,
    Publication,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationResource,
    PublicationUpcoming,
    ViewCount,
    i18nJson,
)
from data_gov_my.serializers import (
    FormDataSerializer,
    PublicationDetailSerializer,
    PublicationDocumentationSerializer,
    PublicationSerializer,
    PublicationUpcomingSerializer,
    ViewCountSerializer,
    i18nSerializer,
)
from data_gov_my.utils import cron_utils
from data_gov_my.utils.meta_builder import GeneralMetaBuilder

env = environ.Env()
environ.Env.read_env()

"""
Endpoint for all single charts
"""

logging.basicConfig(level=logging.INFO)


class AUTH_TOKEN(APIView):
    def post(self, request, format=None):
        try:
            b_unicode = request.body.decode("utf-8")
            auth_token = json.loads(b_unicode).get("ROLLING_TOKEN", None)
            if (not auth_token) or (not isinstance(auth_token, str)):
                raise ParseError("AUTH_TOKEN must be a valid str.")

            auth_token = f"Bearer {auth_token}"
            cur_time = datetime.now(tz=get_current_timezone())
            defaults = {"value": auth_token, "timestamp": cur_time}
            AuthTable.objects.update_or_create(key="AUTH_TOKEN", defaults=defaults)
            cache.set("AUTH_KEY", auth_token)
        except Exception as e:
            return JsonResponse({"status": 400, "message": str(e)}, status=400)

        return JsonResponse(
            {"status": 200, "message": "Auth token received."}, status=200
        )


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

            data_last_updated = meta.get("data_last_updated", None)
            data_as_of = chart_data["data_as_of"]
            chart_data = chart_data["data"]

            #  TEMP FIX
            temp = {}
            if chart_type == "timeseries_chart" and "constants" in chart_variables:
                const_keys = chart_variables["constants"]
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
            overall_data["data_last_updated"] = data_last_updated

        return JsonResponse(overall_data, safe=False)


class UPDATE(APIView):
    def post(self, request, format=None):
        thread = Thread(target=GeneralMetaBuilder.selective_update)
        thread.start()
        return Response(status=status.HTTP_200_OK)


class DASHBOARD(APIView):
    def get(self, request: request.Request, format=None):
        param_list = request.query_params

        if "dashboard" in param_list:
            res = handle_request(param_list)
            res = handle.dashboard_additional_handling(param_list, res)
            return JsonResponse(res, safe=False, status=200)
        else:
            return JsonResponse(
                {
                    status: status.HTTP_400_BAD_REQUEST,
                    "message": "Missing 'dashboard' query parameter.",
                },
                status=status.HTTP_400_BAD_REQUEST,
            )


class DATA_VARIABLE(APIView):
    def get(self, request, format=None):
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


class DROPDOWN(APIView):
    def get(self, request: request.Request, format=None):
        param_list = request.query_params

        if "dashboard" in param_list:
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
                limit = int(limit)
                filtered_res = filtered_res[:limit]
                info["limit"] = limit

            res = {"info": info, "data": filtered_res}
            return JsonResponse(res, safe=False)
        else:
            return JsonResponse({}, safe=False)


class I18N(APIView):
    def get(self, request, *args, **kwargs):
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
        serializer = i18nSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.validated_data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def patch(self, request, *args, **kwargs):
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


class FORMS(generics.ListAPIView):
    serializer_class = FormDataSerializer

    def post(self, request, *args, **kwargs):
        # get FormTemplate instance by request query param, then validate & store new form data
        form_type = kwargs.get("form_type")
        template = FormTemplate.objects.get(form_type=form_type)
        form_data: FormData = template.create_form_data(request.data)

        if template.can_send_email():
            recipient = form_data.get_recipient()
            if recipient:
                email = mail.send(
                    recipients=recipient,
                    template=template.email_template,
                    language=form_data.language,
                    priority="now",
                    context=form_data.form_data,
                )
                form_data.email = email
                form_data.save(update_fields=["email"])

        if form_data.email:
            return JsonResponse(
                data={
                    "Email Recipient": form_data.email.to,
                    "Email Status": form_data.email.STATUS_CHOICES[
                        form_data.email.status
                    ][1],
                },
                status=status.HTTP_200_OK,
            )

    def get_queryset(self):
        """
        This view should return a list of all the form data based on the form type (e.g. /mods)
        """
        form_type = self.kwargs["form_type"]
        return FormData.objects.filter(form_type=form_type)

    def delete(self, request, *args, **kwargs):
        queryset = Email.objects.filter(
            formdata__form_type=kwargs["form_type"]
        )  # query email for cascading deletes
        count, deleted = queryset.delete()
        return JsonResponse(
            data={"Total deleted": count, "Data deleted": deleted},
            status=status.HTTP_200_OK,
        )


class VIEW_COUNT(APIView):
    def get(self, request, format=None):
        return JsonResponse(
            ViewCountSerializer(ViewCount.objects.all(), many=True).data, safe=False
        )

    def post(self, request, format=None):
        id = request.query_params.get("id", None)
        type = request.query_params.get("type", None)
        metric = request.query_params.get("metric", None)

        default_values = {
            "type": ["dashboard", "data-catalogue"],
            "metric": [
                "view_count",
                "download_png",
                "download_csv",
                "download_svg",
                "download_parquet",
            ],
        }

        # Checks if all parameters have values
        if not all([id, type, metric]):
            return JsonResponse(
                {
                    "status": 400,
                    "message": "Parameters id, type and metric must be supplied",
                },
                status=400,
            )

        # Checks if parameter 'type' and 'metric' has appropriate values
        for k, v in default_values.items():
            if request.query_params.get(k) not in v:
                pos_values = ", ".join(v)
                return JsonResponse(
                    {
                        "status": 400,
                        "message": f"Parameter '{k}' has to hold either values : {pos_values}",
                    },
                    status=400,
                )

        res = vcc.ViewCountCache().handle_viewcount(id=id, type=type, metric=metric)

        if type == "dashboard":
            for i in ["csv", "parquet", "png", "svg"]:
                res.pop(f"download_{i}")

        return JsonResponse(res, status=200, safe=False)


class UPDATE_VIEW_COUNT(APIView):
    def post(self, request, format=None):
        res = vcc.ViewCountCache.update_cache()

        if res:
            return JsonResponse({"update": "successful"}, status=200, safe=False)

        return JsonResponse({"update": "failed"}, status=500, safe=False)


class PublicationPagination(PageNumberPagination):
    page_size = 15
    page_size_query_param = "page_size"
    max_page_size = 1000


class PUBLICATION(generics.ListAPIView):
    serializer_class = PublicationSerializer
    pagination_class = PublicationPagination

    def get_queryset(self):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        return Publication.objects.filter(language=language)

    def filter_queryset(self, queryset):
        # apply filters
        pub_type = self.request.query_params.get("pub_type")
        if pub_type:
            queryset = queryset.filter(publication_type__iexact=pub_type)

        frequency = self.request.query_params.get("frequency")
        if frequency:
            queryset = queryset.filter(frequency__iexact=frequency)

        geography = self.request.query_params.get("geography")
        if geography:
            geography = geography.split(",")
            queryset = queryset.filter(geography__contains=geography)

        demography = self.request.query_params.get("demography")
        if demography:
            demography = demography.split(",")
            queryset = queryset.filter(demography__contains=demography)
        return queryset


class PUBLICATION_RESOURCE(generics.RetrieveAPIView):
    serializer_class = PublicationDetailSerializer

    def get_object(self):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        pub_object = get_object_or_404(
            Publication, publication_id=self.kwargs["id"], language=language
        )
        return pub_object


@api_view(["POST"])
def publication_resource_download(request: request.Request):
    if request.query_params.get("documentation_type", "").lower() == "true":
        model = PublicationDocumentationResource
    else:
        model = PublicationResource
    pub_id = request.data.get("publication_id")
    resource_id = request.data.get("resource_id")
    resources = model.objects.filter(
        publication__publication_id=pub_id, resource_id=resource_id
    )
    updated = resources.update(downloads=F("downloads") + 1)
    downloads = resources.values_list("downloads", flat=True)

    if len(set(downloads)) > 1:
        return Response(
            data={
                "error": f"Inconsistent download count between en and bm resources. ({downloads})"
            },
            status=status.HTTP_409_CONFLICT,
        )
    elif not downloads:
        return Response(
            data={"details": "No relevant publication resource found."},
            status=status.HTTP_404_NOT_FOUND,
        )

    new_download_count = downloads.first()

    return Response(
        data={"download": new_download_count},
        status=status.HTTP_200_OK,
    )


class PUBLICATION_DROPDOWN(APIView):
    def get(self, request: request.Request, format=None):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        return JsonResponse(
            list(
                Publication.objects.filter(language=language)
                .order_by("publication_type")
                .values("publication_type", "publication_type_title")
                .distinct()
            ),
            safe=False,
            status=200,
        )


class PUBLICATION_DOCS(generics.ListAPIView):
    serializer_class = PublicationDocumentationSerializer
    pagination_class = PublicationPagination
    filter_backends = [SearchFilter]
    search_fields = ["title", "description"]

    def get_queryset(self):
        language = self.request.query_params.get("language")
        doc_type = self.kwargs["doc_type"]
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        return PublicationDocumentation.objects.filter(
            language=language, documentation_type=doc_type
        )


class PUBLICATION_DOCS_RESOURCE(generics.RetrieveAPIView):
    serializer_class = PublicationDetailSerializer

    def get_object(self):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        pub_object = get_object_or_404(
            PublicationDocumentation,
            publication_id=self.kwargs["id"],
            language=language,
        )
        return pub_object


class PUBLICATION_UPCOMING_CALENDAR(APIView):
    def get(self, request: request.Request, format=None):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        queryset = PublicationUpcoming.objects.filter(language=language)

        # filter start and end date
        start = self.request.query_params.get("start")
        end = self.request.query_params.get("end")
        if start:
            queryset = queryset.filter(release_date__gte=start)
        if end:
            queryset = queryset.filter(release_date__lte=end)

        # process into dict response
        queryset = queryset.order_by("release_date")
        res = {}
        for date, group in groupby(queryset, lambda x: x.release_date):
            res[str(date)] = PublicationUpcomingSerializer(group, many=True).data

        return JsonResponse(data=res, status=200)


class PUBLICATION_UPCOMING_LIST(generics.ListAPIView):
    serializer_class = PublicationUpcomingSerializer
    pagination_class = PublicationPagination

    def get_queryset(self):
        language = self.request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        return PublicationUpcoming.objects.filter(language=language)

    def filter_queryset(self, queryset):
        # apply filters
        pub_type = self.request.query_params.get("pub_type")
        if pub_type:
            queryset = queryset.filter(publication_type__iexact=pub_type)

        # filter start and end date
        start = self.request.query_params.get("start")
        end = self.request.query_params.get("end")
        if start:
            queryset = queryset.filter(release_date__gte=start)
        if end:
            queryset = queryset.filter(release_date__lte=end)
        return queryset


class PUBLICATION_UPCOMING_DROPDOWN(APIView):
    def get(self, request: request.Request, format=None):
        language = request.query_params.get("language")
        if language not in ["en-GB", "ms-MY"]:
            raise ParseError(
                detail=f"Please ensure `language` query parameter is provided with either en-GB or ms-MY as the value."
            )
        return JsonResponse(
            list(
                PublicationUpcoming.objects.filter(language=language)
                .order_by("publication_type")
                .values("publication_type", "publication_type_title")
                .distinct()
            ),
            safe=False,
            status=200,
        )


"""
Checks which filters have been applied for the data-catalog
"""


def get_filters_applied(param_list):
    default_params = {
        "period": "",
        "geography": [],
        "demography": [],
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
        elif k == "geography":
            for i in v:
                query &= Q(geography__contains=i)
        elif k == "demography":
            for i in v:
                query &= Q(demography__contains=i)
        elif k == "source":
            for i in v:
                query &= Q(data_source__contains=i)
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
    catalog_data = cache.get(var_id)
    exclude_openapi = cache.get(f"{var_id}_openapi")

    if not catalog_data or not exclude_openapi:
        info = get_object_or_404(CatalogJson, id=var_id)
        catalog_data = info.catalog_data
        exclude_openapi = info.exclude_openapi
        cache.set(var_id, catalog_data)
        cache.set(f"{var_id}_openapi", exclude_openapi)

    chart_type = catalog_data["API"]["chart_type"]
    res = data_variable_chart_handler(catalog_data, chart_type, param_list)
    res["exclude_openapi"] = exclude_openapi

    if len(res) == 0:  # If catalogues with the filter isn't found
        return {}
    return res


"""
Handles request for dashboards
"""


def handle_request(param_list: QueryDict, isDashboard=True):
    dbd_name = param_list["dashboard"]
    dbd_info = cache.get("META_" + dbd_name)

    if not dbd_info:
        dbd_info = MetaJson.objects.filter(dashboard_name=dbd_name).values(
            "dashboard_meta"
        )

    params_req = []
    data_last_updated = None

    if len(dbd_info) > 0:
        dbd_info = (
            dbd_info if isinstance(dbd_info, dict) else dbd_info[0]["dashboard_meta"]
        )
        params_req = dbd_info["required_params"]
        params_opt = dbd_info.get("optional_params", [])
        data_last_updated = dbd_info.get("data_last_updated", None)

    res = {"data_last_updated": data_last_updated}
    if (
        all(p in param_list for p in params_req)
        or all(p in param_list for p in params_opt)
        or not isDashboard
    ):
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

                data_as_of = cur_chart_data.get("data_as_of", None)

                if api_type == "static":
                    res[k] = {}
                    if data_as_of:
                        res[k]["data_as_of"] = data_as_of
                    res[k]["data"] = cur_chart_data["data"]
                elif api_type == "dynamic":
                    if len(api_params) > 0:
                        cur_chart_data = get_nested_data(
                            dbd_info, api_params, param_list, cur_chart_data["data"]
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


def get_nested_data(
    dbd_info: dict,
    api_params: list[str],
    param_list: QueryDict,
    data: dict,
):
    for a in api_params:
        optional = a in dbd_info.get("optional_params", [])
        if a in param_list:
            key = param_list[a] if "__FIXED__" not in a else a.replace("__FIXED__", "")
            if key in data:
                data = data[key]
            elif optional:
                data = {}
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
