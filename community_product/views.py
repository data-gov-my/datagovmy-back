from rest_framework import generics
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
    queryset = CommunityProduct.objects.all()  # TODO: order by created_at?
    serializer_class = CommunityProductSerializer
    pagination_class = CommunityProductPagination
