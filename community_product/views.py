from rest_framework import generics, filters
from .models import CommunityProduct
from .serializers import CommunityProductSerializer
from rest_framework.pagination import PageNumberPagination


class CommunityProductPagination(PageNumberPagination):
    page_size = 15
    page_size_query_param = "page_size"
    max_page_size = 100


class CommunityProductCreateView(generics.CreateAPIView):
    queryset = CommunityProduct.objects.all()
    serializer_class = CommunityProductSerializer


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
