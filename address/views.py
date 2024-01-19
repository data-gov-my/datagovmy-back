# views.py
import csv
from io import BytesIO, StringIO, TextIOWrapper
import numpy as np
import pandas as pd
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.views import APIView

from address.tasks import rebuild_address
from .models import Address
from rest_framework.generics import ListAPIView
from rest_framework.response import Response

from .models import Address
from .serializers import AddressSerializer
from django.contrib.postgres.search import TrigramSimilarity, TrigramWordSimilarity
from silk.profiling.profiler import silk_profile


class AddressSearchByAddView(ListAPIView):
    pass


class AddressSearchView(ListAPIView):
    serializer_class = AddressSerializer

    @silk_profile(name="Search Address")
    def get_queryset(self):
        """
        Exact case insensitive match postcode, partial match unit, fuzzy search address.
        """
        queryset = Address.objects.all()

        # result size
        n = 10
        size: str = self.request.query_params.get("size", "")
        if size.isdigit():
            size = int(size)
            n = size

        # full text search (fuzzy)
        address = self.request.query_params.get("address")
        if address:
            queryset = queryset.annotate(
                similarity=TrigramSimilarity("address", address)
            ).order_by("-similarity")

        else:
            # postcode (exact) and unit (contains)
            postcode = self.request.query_params.get("postcode")
            if postcode is not None:
                queryset = queryset.filter(postcode__iexact=postcode)

            unit = self.request.query_params.get("unit")
            if unit is not None:
                temp = queryset.filter(address__istartswith=unit)
                if not queryset.exists():
                    temp = queryset.filter(address__icontains=unit)
                queryset = temp
        return queryset[:n]


class AddressUploadView(APIView):
    PARQUET_LINK = "https://storage.data.gov.my/dashboards/alamat_sample.parquet"

    def post(self, request, *args, **kwargs):
        df = pd.read_parquet(self.PARQUET_LINK)
        df.replace({np.nan: None}, inplace=True)
        address_data = df.to_dict(orient="records")
        address_instances = [Address(**data) for data in address_data]
        Address.objects.all().delete()
        Address.objects.bulk_create(address_instances, batch_size=10000)
        return Response(
            {"message": f"Created {Address.objects.count()} addresses."}, status=200
        )
