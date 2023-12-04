from rest_framework import serializers

from data_request.models import DataRequest


class DataRequestSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataRequest
        fields = "__all__"
