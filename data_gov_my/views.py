import json
import logging
import os
from datetime import datetime, timedelta
from http import HTTPStatus
from itertools import groupby
from threading import Thread

import environ
import requests
from django.core.cache import cache
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.db.models import F, Q, Sum
from django.http import JsonResponse, QueryDict
from django.shortcuts import get_list_or_404, get_object_or_404
from django.utils.html import strip_tags
from django.utils.timezone import get_current_timezone
from jose import jwt
from post_office import mail
from post_office.models import Email
from rest_framework import generics, request, status
from rest_framework.decorators import api_view
from rest_framework.exceptions import ParseError
from rest_framework.filters import SearchFilter
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView

from data_gov_my.api_handling import handle
from data_gov_my.explorers import class_list as exp_class
from data_gov_my.models import (
    AuthTable,
    DashboardJson,
    FormData,
    FormTemplate,
    MetaJson,
    Publication,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationResource,
    PublicationSubscription,
    PublicationUpcoming,
    Subscription,
    i18nJson, PublicationType,
)
from data_gov_my.serializers import (
    FormDataSerializer,
    PublicationDetailSerializer,
    PublicationDocumentationSerializer,
    PublicationSerializer,
    PublicationUpcomingSerializer,
    i18nSerializer,
)
from data_gov_my.utils import triggers
from data_gov_my.utils.email_normalization import normalize_email
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from data_gov_my.utils.publication_helpers import create_token_message
from data_gov_my.utils.throttling import FormRateThrottle

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
            meta = cache.get(f"META_{dbd_name}")

            if not meta:
                meta = MetaJson.objects.filter(dashboard_name=dbd_name).values(
                    "dashboard_meta"
                )[0]["dashboard_meta"]
                cache.set(f"META_{dbd_name}", meta)

            api_params = meta["charts"][chart_name]["api_params"]
            chart_type = meta["charts"][chart_name]["chart_type"]
            api_type = meta["charts"][chart_name]["api_type"]
            chart_variables = meta["charts"][chart_name]["variables"]

            chart_data = cache.get(f"{dbd_name}_{chart_name}")

            if not chart_data:
                chart_data = DashboardJson.objects.filter(
                    dashboard_name=dbd_name, chart_name=chart_name
                ).values("chart_data")[0]["chart_data"]
                cache.set(f"{dbd_name}_{chart_name}", chart_data)

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
            filtered_res = dropdown_lst

            if not filtered_res:
                return JsonResponse({}, safe=False)

            if query := param_list.get("query"):
                query = query.lower()
                if filters := param_list.get("filters"):
                    filters = filters.split(",")
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

            info = {"total": len(filtered_res)}

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


class PublicationTypeSubtypeList(APIView):
    def get(self, request, format=None):
        lang = request.query_params.get("lang")
        pub_type = PublicationType.objects.all().order_by("order")

        data = {}
        if lang == 'ms':
            for p in pub_type:
                data[p.type_bm] = {s.subtype: s.subtype_bm for s in p.publicationsubtype_set.all().order_by("order")}
        else:
            # default to English
            for p in pub_type:
                data[p.type_en] = {s.subtype: s.subtype_en for s in p.publicationsubtype_set.all().order_by("order")}
        return JsonResponse(data, status=status.HTTP_200_OK)



class FORMS(generics.ListAPIView):
    serializer_class = FormDataSerializer

    def get_throttles(self):
        if self.request.method == "POST":
            self.throttle_classes = [FormRateThrottle]
        return super().get_throttles()

    def post(self, request, *args, **kwargs):
        # get FormTemplate instance by request query param, then validate & store new form data
        form_type = kwargs.get("form_type")
        template = get_object_or_404(FormTemplate, form_type=form_type)
        form_data: FormData = template.create_form_data(request.data)

        if template.can_send_email():
            recipient = form_data.get_recipient()
            if recipient:
                email = mail.send(
                    recipients=recipient,
                    template=template.email_template,
                    language=form_data.language,
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
        return (
            Publication.objects.filter(language=language)
            .annotate(total_downloads=Sum("resources__downloads"))
            .order_by("-release_date", "publication_id")
        )

    def filter_queryset(self, queryset):
        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search)
                | Q(description__icontains=search)
                | Q(publication_id__icontains=search)
            )

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


class SubscribePublicationAPIView(APIView):
    def post(self, request):
        publication_type = request.data.get("publication_type")
        email = request.data.get("email")

        if not publication_type or not email:
            return Response(
                {"error": "Provide both `publication_type` and `email` form data."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if publication_type is vaild
        if not Publication.objects.filter(publication_type=publication_type).exists():
            return Response(
                {"error": "Invalid publication_type (does not exist)."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Check if the publication type exists in the database
        subscription, created = PublicationSubscription.objects.get_or_create(
            publication_type=publication_type
        )

        if not created:
            # If the publication type already exists, check if the email is already subscribed
            if email in subscription.emails:
                return Response(
                    {"error": "Email is already subscribed to this publication type."},
                    status=status.HTTP_400_BAD_REQUEST,
                )

        # Append the email to the list of subscribed emails
        try:
            validate_email(email)
        except ValidationError as e:
            return Response(
                {"error": "Invalid email format."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        else:
            subscription.emails.append(email)
            subscription.save()

        return Response(
            {"success": f"Subscribed to {publication_type}."},
            status=status.HTTP_201_CREATED,
        )


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


@api_view(["GET"])
def get_publication_resource_downloads(request):
    cols = ["pub_id", "resource_id", "downloads"]
    queryset = (
        PublicationResource.objects.filter(publication__language="en-GB")
        .annotate(pub_id=F("publication__publication_id"))
        .values(*cols)
        .order_by(*cols)
    )
    return Response(queryset, status=status.HTTP_200_OK)


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
        return (
            PublicationDocumentation.objects.filter(
                language=language, documentation_type=doc_type
            )
            .annotate(total_downloads=Sum("resources__downloads"))
            .order_by("publication_id")
        )

    def filter_queryset(self, queryset):
        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(
                Q(title__icontains=search)
                | Q(description__icontains=search)
                | Q(publication_id__icontains=search)
            )
        return queryset


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
        search = self.request.query_params.get("search")
        if search:
            queryset = queryset.filter(
                Q(publication_title__icontains=search)
                | Q(publication_type_title__icontains=search)
                | Q(publication_id__icontains=search)
            )

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


def handle_request(param_list: QueryDict, isDashboard=True):
    """
    Handles request for dashboards
    """
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
        cache.set(f"META_{dbd_name}", dbd_info)
        params_req = dbd_info["required_params"]
        params_opt = dbd_info.get("optional_params", [])
        data_last_updated = dbd_info.get("data_last_updated", None)
        data_next_update = dbd_info.get("data_next_update", None)

    res = {
        "data_last_updated": data_last_updated,
        "data_next_update": data_next_update,
    }
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


def get_nested_data(
        dbd_info: dict,
        api_params: list[str],
        param_list: QueryDict,
        data: dict,
):
    """
    Slices dictionary,
    based on keys within dictionary"""
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


class CheckSubscriptionView(APIView):
    def post(self, request):
        email = request.data["email"]
        email = normalize_email(email)
        try:
            Subscription.objects.get(email=email)
            return Response({'message': f'Email does exist'}, status=status.HTTP_200_OK)
        except Subscription.DoesNotExist:
            sub = Subscription.objects.create(email=email, publications=[])
            validity = datetime.now() + timedelta(minutes=5)  # token valid for 5 mins
            jwt_token = jwt.encode({
                'sub': email,
                'validity': int(validity.timestamp()),
            }, os.getenv("WORKFLOW_TOKEN"))
            html_message = create_token_message(jwt_token=jwt_token)
            message = strip_tags(html_message)
            mail.send(
                sender='OpenDOSM <notif@opendosm.my>',
                recipients=[sub.email],
                subject='Verify Your Email',
                html_message=html_message,
                message=message,
                priority='now'
            )
            return Response({'message': f"Email does not exist"}, status=status.HTTP_200_OK)


class SubscriptionView(APIView):
    def put(self, request):
        token = request.headers.get("Authorization", None)
        if not token:
            token = request.META["headers"]["Authorization"]

        decoded_token = jwt.decode(token, os.getenv("WORKFLOW_TOKEN"))
        email = decoded_token["sub"]
        email = normalize_email(email)

        subscriber = Subscription.objects.get(email=email)
        publication_list = request.data.getlist("publications", None)
        subscriber.publications = publication_list
        subscriber.save()
        
        # make a POST request to tinybird
        try:
            r = requests.post(
                url=os.getenv("SUBSCRIPTION_TINYBIRD_URL"),
                headers={"Authorization": f"Bearer {os.getenv('SUBSCRIPTION_TINYBIRD_TOKEN')}"},
                data=json.dumps({
                    'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f"),
                    'email': email,
                    'publications': publication_list
                })
            )
            if r.status_code != 202:
                triggers.send_telegram(f'STAGING TINYBIRD STATUS: {r.status_code}\nTEXT: {r.text}')
            if r.json()["quarantined_rows"]:
                triggers.send_telegram(f'STAGING TINYBIRD STATUS: {r.status_code}\nQUARANTINED ROWS: {r.json()["quarantined_rows"]}')

        except requests.exceptions.RequestException as e:
            print(f'tinybird error: {e}')
            triggers.send_telegram(f'STAGING TINYBIRD ERROR: {e}')

        return Response({'email': subscriber.email, 'message': 'Subscriptions updated.'}, HTTPStatus.OK)

    def get(self, request):
        token = request.headers.get("Authorization", None)
        if not token:
            token = request.META["headers"]["Authorization"]
        decoded_token = jwt.decode(token, os.getenv("WORKFLOW_TOKEN"))
        email = decoded_token["sub"]
        email = normalize_email(email)

        sub = Subscription.objects.get(email=email).publications
        return Response({'email': email, 'data': sub}, HTTPStatus.OK)


class TokenRequestView(APIView):
    def post(self, request):
        to = request.data.get("email", None)
        if not to:
            Response(status=HTTPStatus.BAD_REQUEST)

        email = normalize_email(to)
        try:
            sub = Subscription.objects.get(email=email)
            validity = datetime.now() + timedelta(minutes=5)  # token valid for 5 mins
            jwt_token = jwt.encode({
                'sub': email,
                'validity': int(validity.timestamp()),
            }, os.getenv("WORKFLOW_TOKEN"))
            html_message = create_token_message(jwt_token=jwt_token)
            message = strip_tags(html_message)
            mail.send(
                sender='OpenDOSM <notif@opendosm.my>',
                recipients=[sub.email],
                subject='Verify Your Email',
                message=message,
                html_message=html_message,
                priority='now'
            )
            return Response({'message': 'User subscribed. Email with login token sent'}, status=HTTPStatus.OK)
        except Subscription.DoesNotExist:
            return Response({'message': 'Not subscribed. Proceed to sign up.'}, status=HTTPStatus.OK)


class TokenVerifyView(APIView):
    def post(self, request):
        token = request.data.get("token", None)

        try:
            decoded_token = jwt.decode(token, os.getenv("WORKFLOW_TOKEN"))
            email = decoded_token["sub"]
            email = normalize_email(email)
        except Exception as e:
            return Response({'message': f'Error {e}'}, status=HTTPStatus.OK)

        try:
            sub = Subscription.objects.get(email=email)
            validity_timestamp = decoded_token["validity"]
            if datetime.now() < datetime.fromtimestamp(validity_timestamp):
                return Response({'message': 'Token verified.', 'email': sub.email}, status=HTTPStatus.OK)
            else:
                return Response({'message': 'Token expired.', 'email': sub.email}, status=HTTPStatus.OK)
        except Subscription.DoesNotExist:
            return Response({'message': 'Email not verified.'}, status=HTTPStatus.OK)


class TokenManageSubscriptionView(APIView):
    def post(self, request):
        token = request.data.get("token", None)
        decoded_token = jwt.decode(token, os.getenv("WORKFLOW_TOKEN"))
        email = decoded_token["sub"]
        email = normalize_email(email)

        # Clear all existing subscription - can make it cleaner?
        for publication in PublicationSubscription.objects.all():
            if email in publication.emails:
                publication.emails.remove(email)
                publication.save()

        publications_list = request.POST.getlist("publication_type")
        if type(publications_list) is not list:
            return Response(
                {"error": f"Type `publication_type` should be a list. It's {type(publications_list)}"},
                status=status.HTTP_400_BAD_REQUEST,
            )
        for publication in publications_list:
            if not publication or not email:
                return Response(
                    {"error": "Provide both `publication_type` and `email` data."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            try:
                validate_email(email)
            except ValidationError as e:
                return Response(
                    {"error": "Invalid email format."},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            else:
                pub_sub = PublicationSubscription.objects.get(publication_type=publication)
                pub_sub.emails.append(email)
                pub_sub.save()

        return Response(
            {"success": f"Subscribed to {publications_list}."},
            status=status.HTTP_201_CREATED,
        )
