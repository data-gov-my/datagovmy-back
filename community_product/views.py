from rest_framework import generics
from .models import CommunityProduct
from .serializers import CommunityProductSerializer


class CommunityProductListCreateView(generics.ListCreateAPIView):
    queryset = CommunityProduct.objects.all()
    serializer_class = CommunityProductSerializer


class CommunityProductDetailView(generics.RetrieveUpdateAPIView):
    queryset = CommunityProduct.objects.all()
    serializer_class = CommunityProductSerializer
