# views.py
import csv
from io import BytesIO, StringIO, TextIOWrapper
import numpy as np
import pandas as pd
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.views import APIView
from .models import Address
from rest_framework.generics import ListAPIView
from rest_framework.response import Response

from .models import Address
from .serializers import AddressSerializer
from django.contrib.postgres.search import TrigramSimilarity, TrigramWordSimilarity


class AddressSearchByAddView(ListAPIView):
    pass


class AddressSearchView(ListAPIView):
    serializer_class = AddressSerializer

    def get_queryset(self):
        """
        Exact case insensitive match postcode, partial match unit,
        """
        queryset = Address.objects.all()

        # result size
        n = 10
        size: str = self.request.query_params.get("size", "")
        if size.isdigit():
            size = int(size)
            n = size

        # postcode (exact) and unit (contains)
        postcode = self.request.query_params.get("postcode")
        if postcode is not None:
            queryset = queryset.filter(postcode=postcode)

        unit = self.request.query_params.get("unit")
        if unit is not None:
            queryset = queryset.filter(unit__icontains=unit)

        # full text search (fuzzy)
        if not postcode and not unit:
            address = self.request.query_params.get("address")
            address = ",".join(
                [portion.strip() for portion in address.split(",")]
            ).lower()

            if address is None:
                return Address.objects.none()
            if address is not None:
                # Use trigram similarity for fuzzy search
                queryset = (
                    queryset.annotate(
                        similarity=TrigramSimilarity("combined_address", address)
                    )
                    .filter(similarity__gt=0.2)
                    .order_by("-similarity")
                )

        return queryset[:n]


class AddressUploadView(APIView):
    def post(self, request, *args, **kwargs):
        file = request.data["file"]
        df = pd.read_csv(file)
        df["combined_address"] = (
            (
                df["unit"].fillna("")
                + ","
                + df["namaBangunan"].fillna("")
                + ","
                + df["namaJalan"].fillna("")
                + ","
                + df["lokaliti"]
                + ","
                + df["poskod"].astype(str)
                + ","
                + df["bandar"]
                + ","
                + df["negeri"]
                + ","
                + df["negara"]
            )
            .str.lower()
            .str.strip(",")
        )
        df.replace({np.nan: None}, inplace=True)

        address_data = df.to_dict(orient="records")
        address_instances = [Address(**data) for data in address_data]
        Address.objects.all().delete()
        Address.objects.bulk_create(address_instances, batch_size=20000)
        return Response(
            {"message": f"Created {Address.objects.count()} addresses."}, status=200
        )
