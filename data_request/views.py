# Create your views here.
import logging
from django.db.models import Q
from django.utils import translation
from rest_framework import generics, status
from rest_framework.decorators import api_view
from rest_framework.response import Response
from post_office import mail
from data_request.models import DataRequest
from data_request.serializers import DataRequestSerializer


class DataRequestCreateAPIView(generics.CreateAPIView):
    serializer_class = DataRequestSerializer
    FORM_TYPE = "data_request_submitted"

    def create(self, request, *args, **kwargs):
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
            data = request.data.dict()
            data["language"] = email_lang
            serializer = self.get_serializer(data=data)
            serializer.is_valid(raise_exception=True)
            self.perform_create(serializer)

        headers = self.get_success_headers(serializer.data)
        recipient = serializer.validated_data.get("email")
        # FIXME: use proper celery worker to queue send emails
        try:
            context = serializer.data
            context["name"] = data.get("name")
            email = mail.send(
                recipients=recipient,
                language=email_lang,
                priority="now",
                template=self.FORM_TYPE,
                context=context,
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

    queryset = DataRequest.objects.exclude(status="submitted")
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

    translation.activate(lang)

    return Response(DataRequestSerializer(queryset, many=True).data)
