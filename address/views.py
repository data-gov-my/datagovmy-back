# views.py
import csv
from io import BytesIO, StringIO, TextIOWrapper
from rest_framework.parsers import FileUploadParser
from rest_framework.response import Response
from rest_framework.views import APIView

from address.tasks import rebuild_address
from .models import Address
from rest_framework.generics import ListAPIView
from rest_framework.response import Response

from .models import Address
from .serializers import AddressSerializer


class AddressSearchByAddView(ListAPIView):
    pass


class AddressSearchByPostcodeView(ListAPIView):
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

        rebuild_address.delay(file_content)

        return Response({"message": "Table rebuilding in process..."}, status=200)
