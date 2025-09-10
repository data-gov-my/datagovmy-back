import logging

from django.conf import settings
from django.http import QueryDict
from django.utils import translation
from post_office import mail
from rest_framework import filters, generics, status
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from data_gov_my.utils.throttling import FormRateThrottle

from .models import CommunityProduct
from .serializers import CommunityProductListSerializer, CommunityProductSerializer


class CommunityProductPagination(PageNumberPagination):
    page_size = 9
    page_size_query_param = "page_size"
    max_page_size = 100


class CommunityProductCreateView(generics.CreateAPIView):
    queryset = CommunityProduct.objects.all()
    serializer_class = CommunityProductSerializer
    FORM_TYPE = "community_product_submitted"
    throttle_classes = [FormRateThrottle]

    def create(self, request, *args, **kwargs):
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language

        if language not in ["en", "ms"]:
            return Response(
                {"error": "language param should be `en`, `ms` or `bm` only."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        with translation.override(language):
            data = (
                request.data.dict()
                if isinstance(request.data, QueryDict)
                else request.data
            )
            data["language"] = language
            serializer = self.get_serializer(data=data)
            serializer.is_valid(raise_exception=True)
            self.perform_create(serializer)
            headers = self.get_success_headers(serializer.data)

            # send email
            try:
                email = mail.send(
                    sender=settings.DATA_GOV_MY_FROM_EMAIL,
                    recipients=serializer.data.get("email"),
                    language=language,
                    template=self.FORM_TYPE,
                    context=serializer.data,
                    backend="datagovmy_ses",
                )

            except Exception as e:
                logging.error(e)

            return Response(
                serializer.data, status=status.HTTP_201_CREATED, headers=headers
            )


class CommunityProductDetailAPIView(generics.RetrieveAPIView):
    serializer_class = CommunityProductSerializer
    lookup_field = "id"

    def get_queryset(self):
        language = self.request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        translation.activate(language)
        return CommunityProduct.objects.filter(status="approved")


class CommunityProductListView(generics.ListAPIView):
    serializer_class = CommunityProductListSerializer
    pagination_class = CommunityProductPagination
    filter_backends = [filters.SearchFilter]
    search_fields = ["product_name", "product_description"]

    def list(self, request, *args, **kwargs):
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        with translation.override(language):
            return super().list(request, *args, **kwargs)

    def get_queryset(self):
        queryset = CommunityProduct.objects.filter(status="approved").order_by(
            "-date_approved"
        )

        # Filter by product_year if provided in the query parameters
        product_year = self.request.query_params.get("product_year", None)
        if product_year:
            queryset = queryset.filter(product_year=product_year)

        # Filter by product_type if provided in the query parameters
        product_type = self.request.query_params.get("product_type", None)
        if product_type:
            queryset = queryset.filter(product_type=product_type)

        return queryset
