from django.http import QueryDict
from django.utils import translation
from rest_framework import filters, generics, status
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response

from .models import CommunityProduct
from .serializers import CommunityProductSerializer


class CommunityProductPagination(PageNumberPagination):
    page_size = 15
    page_size_query_param = "page_size"
    max_page_size = 100


class CommunityProductCreateView(generics.CreateAPIView):
    queryset = CommunityProduct.objects.all()
    serializer_class = CommunityProductSerializer

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
            serializer = self.get_serializer(data=data)
            serializer.is_valid(raise_exception=True)
            self.perform_create(serializer)
            headers = self.get_success_headers(serializer.data)
            return Response(
                serializer.data, status=status.HTTP_201_CREATED, headers=headers
            )


class CommunityProductDetailView(generics.ListAPIView):
    serializer_class = CommunityProductSerializer
    pagination_class = CommunityProductPagination
    filter_backends = [filters.SearchFilter]
    search_fields = ["product_name", "product_description"]

    def get_queryset(self):
        queryset = CommunityProduct.objects.all()

        # Filter by product_year if provided in the query parameters
        product_year = self.request.query_params.get("product_year", None)
        if product_year:
            queryset = queryset.filter(product_year=product_year)

        # Filter by product_type if provided in the query parameters
        product_type = self.request.query_params.get("product_type", None)
        if product_type:
            queryset = queryset.filter(product_type=product_type)

        return queryset
