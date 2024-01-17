# views.py
import csv
from io import BytesIO, StringIO, TextIOWrapper
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.views import APIView
from .models import Address
from rest_framework.generics import ListAPIView
from rest_framework.response import Response

from .models import Address
from .serializers import AddressSerializer


class AddressSearchView(ListAPIView):
    serializer_class = AddressSerializer

    def get_queryset(self):
        postcode = self.kwargs.get("postcode", None)

        if postcode is not None:
            return Address.objects.filter(postcode=postcode)

        return Address.objects.none()


class AddressUploadView(APIView):
    def post(self, request, *args, **kwargs):
        file = request.data["file"]

        file_content = file.read().decode("utf-8")

        # Create a CSV reader
        csv_reader = csv.reader(file_content.splitlines())

        # Assuming the first row of the CSV file contains column headers
        headers = next(csv_reader)

        # Create a list to store Address instances
        address_instances = []

        # Iterate through the remaining rows and create/update Address instances
        for row in csv_reader:
            address_data = dict(zip(headers, row))
            # Create an Address instance without saving it to the database
            address_instance = Address(**address_data)
            address_instances.append(address_instance)

        # Use bulk_create to efficiently create/update all instances in one query
        Address.objects.bulk_create(address_instances, batch_size=20000)

        return Response({"message": "Table rebuilt successfully"}, status=200)
