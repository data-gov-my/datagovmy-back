# Create your views here.
import ast
import logging
import os

import pandas as pd
from django.conf import settings
from django.db.models import Count, Q
from django.http import QueryDict
from django.shortcuts import get_object_or_404
from django.utils import translation
from post_office import mail
from rest_framework import generics, request, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from data_gov_my.utils.throttling import FormRateThrottle
from data_request.models import Agency, DataRequest, DataRequestAdminEmail
from data_request.serializers import (
    AgencySerializer,
    DataRequestSerializer,
    SubscriptionSerializer,
)

class SubscriptionCreateAPIView(generics.CreateAPIView):
    serializer_class = SubscriptionSerializer
    FORM_TYPE = "data_request_subscription"

    def perform_create(self, serializer):
        ticket_id = self.kwargs.get("ticket_id")
        data_request = get_object_or_404(DataRequest, ticket_id=ticket_id)
        # Check if the email already exists in data_request.subscriptions
        email = serializer.validated_data["email"]

        if data_request.status != "under_review":
            raise ValidationError(
                {"status": "You cannot subscribe to tickets that are not under review."}
            )

        if data_request.subscription_set.filter(email=email).exists():
            raise ValidationError(
                {"email": "This email is already in use for this data request ticket."}
            )

        subscription = serializer.save()
        data_request.subscription_set.add(subscription)
        bcc_list = [email.email for email in DataRequestAdminEmail.objects.all()]

        # send email to notify subscription
        try:
            mail.send(
                sender=settings.DEFAULT_FROM_EMAIL_DATA_REQUEST,
                recipients=email,
                bcc=bcc_list,
                language=serializer.validated_data["language"],
                template=self.FORM_TYPE,
                context={
                    "ticket_id": data_request.ticket_id,
                    "name": serializer.validated_data["name"],
                    "email": serializer.validated_data["email"],
                    "institution": serializer.validated_data.get("institution"),
                    "dataset_title": (
                        data_request.dataset_title_en
                        if serializer.validated_data["language"] == "en-GB"
                        else data_request.dataset_title_ms
                    ),
                    "agency": data_request.agency,
                },
                backend="data_request",
            )
        except Exception as e:
            logging.error(e)


class DataRequestCreateAPIView(generics.CreateAPIView):
    serializer_class = DataRequestSerializer
    FORM_TYPE = "data_request_submitted"
    throttle_classes = [FormRateThrottle]

    def create(self, request: request.Request, *args, **kwargs):
        # Determine the language from the query parameters
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language

        if language not in ["en", "ms"]:
            return Response(
                {"error": "language param should be `en`, `ms` or `bm` only."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        # Set the language for translation
        email_lang = "en-GB" if language == "en" else "ms-MY"
        with translation.override(language):
            data = (
                request.data.dict()
                if isinstance(request.data, QueryDict)
                else request.data
            )
            data["language"] = email_lang
            serializer = self.get_serializer(data=data)
            serializer.is_valid(raise_exception=True)
            self.perform_create(serializer)

        headers = self.get_success_headers(serializer.data)
        recipient = serializer.validated_data.get("email")
        bcc_list = [email.email for email in DataRequestAdminEmail.objects.all()]

        # FIXME: use proper celery worker to queue send emails
        try:
            context = serializer.data
            context["name"] = data.get("name")
            email = mail.send(
                sender=settings.DEFAULT_FROM_EMAIL_DATA_REQUEST,
                recipients=recipient,
                language=email_lang,
                template=self.FORM_TYPE,
                context=context,
                backend="data_request",
            )
            email = mail.send(
                sender=settings.DEFAULT_FROM_EMAIL_DATA_REQUEST,
                recipients=bcc_list,
                language=email_lang,
                template=self.FORM_TYPE,
                context=context,
                backend="data_request",
            )
        except Exception as e:
            logging.error(e)

        return Response(
            serializer.data, status=status.HTTP_201_CREATED, headers=headers
        )

    def get_queryset(self):
        return DataRequest.objects.all()


@api_view(["GET"])
def list_data_request(request):
    lang = request.query_params.get("language", "en")
    lang = "ms" if lang == "bm" else lang
    if lang not in ["en", "ms"]:
        return Response(
            {"error": "language param should be `en`, `ms` or `bm` only."},
            status=status.HTTP_400_BAD_REQUEST,
        )

    ticket_id = request.query_params.get("ticket_id", None)
    ticket_status = request.query_params.get("status", None)
    query = request.query_params.get("query", None)

    queryset = DataRequest.objects.exclude(status="submitted").prefetch_related(
        "published_data"
    )
    if ticket_id:
        if not ticket_id.isdigit():
            return Response(
                {"error": "ticket_id must be an integer."},
                status=status.HTTP_400_BAD_REQUEST,
            )
        queryset = queryset.filter(ticket_id=ticket_id)
    if ticket_status:
        if ticket_status == "submitted":
            return Response(
                {"message": "Tickets with `submitted` status are not public."},
                status.HTTP_403_FORBIDDEN,
            )
        queryset = queryset.filter(status=ticket_status)
    if query:
        queryset = queryset.filter(
            Q(dataset_title__icontains=query) | Q(dataset_description__icontains=query)
        )

    queryset = queryset.annotate(total_subscribers=Count("subscription"))

    translation.activate(lang)

    return Response(DataRequestSerializer(queryset, many=True).data)


# Handle Agency population


class AgencyListAPIView(generics.ListAPIView):
    serializer_class = AgencySerializer

    def list(self, request, *args, **kwargs):
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        translation.activate(language)

        return super().list(request, *args, **kwargs)

    def get_queryset(self):
        return Agency.objects.all()


class AgencyCreateAPIView(generics.CreateAPIView):
    AGENCY_PARQUET = "https://storage.data.gov.my/agencies.parquet"

    def post(self, request, *args, **kwargs):
        df = pd.read_parquet(self.AGENCY_PARQUET)
        df["emails"] = df["emails"].apply(
            lambda x: ast.literal_eval(x.decode("utf-8")) if isinstance(x, bytes) else x
        )

        acronym_lst = df["acronym"].tolist()
        agency_objects = [Agency(**row) for row in df.to_dict("records")]
        update_or_created_agencies = Agency.objects.bulk_create(
            agency_objects,
            update_conflicts=True,
            unique_fields=["acronym"],
            update_fields=["name_en", "name_ms", "emails"],
        )

        _, deleted = Agency.objects.exclude(acronym__in=acronym_lst).delete()

        return Response(
            {
                "detail": f"Bulk update/create {len(update_or_created_agencies)} agencies successful.",
                "deleted": deleted,
            },
            status=status.HTTP_201_CREATED,
        )
