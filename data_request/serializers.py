from rest_framework import serializers
from data_catalogue.models import DataCatalogueMeta

from data_request.models import DataRequest


class DataCatalogueMetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataCatalogueMeta
        fields = ["id", "title", "description", "data_source", "data_as_of"]


class DataRequestSerializer(serializers.ModelSerializer):
    published_data = DataCatalogueMetaSerializer(many=True, read_only=True)

    class Meta:
        model = DataRequest
        fields = [
            "name",
            "email",
            "institution",
            "dataset_title",
            "dataset_description",
            "agency",
            "purpose_of_request",
            "status",
            "rejection_reason",
            "published_data",
        ]
