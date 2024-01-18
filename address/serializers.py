from rest_framework import serializers
from .models import Address


class AddressSerializer(serializers.ModelSerializer):
    lat = serializers.DecimalField(
        max_digits=10, decimal_places=7, coerce_to_string=False
    )
    lon = serializers.DecimalField(
        max_digits=10, decimal_places=7, coerce_to_string=False
    )

    class Meta:
        model = Address
        fields = [
            "address",
            "district",
            "subdistrict",
            "locality",
            "parlimen",
            "dun",
            "lat",
            "lon",
            "postcode",
        ]
