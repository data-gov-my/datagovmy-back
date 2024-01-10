from rest_framework import serializers

from data_catalogue.models import DataCatalogueMeta
from data_request.models import Agency, DataRequest, Subscription


class DataCatalogueMetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataCatalogueMeta
        fields = ["id", "title", "description", "data_source", "data_as_of"]


class SubscriptionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Subscription
        fields = "__all__"


class DataRequestSerializer(serializers.ModelSerializer):
    name = serializers.CharField(write_only=True)
    email = serializers.EmailField(write_only=True)
    institution = serializers.CharField(
        write_only=True, required=False, allow_blank=True
    )
    language = serializers.CharField(write_only=True)
    total_subscribers = serializers.IntegerField(read_only=True)
    published_data = DataCatalogueMetaSerializer(many=True, read_only=True)

    def create(self, validated_data):
        """
        Handle creating the data request instance, and also linking its first subscriber.
        """
        subscriber_data = {
            "name": validated_data.pop("name"),
            "email": validated_data.pop("email"),
            "institution": validated_data.pop("institution", ""),
            "language": validated_data.pop("language"),
        }
        created_data_request = super().create(validated_data)
        Subscription.objects.create(
            data_request=created_data_request, **subscriber_data
        )
        return created_data_request

    class Meta:
        model = DataRequest
        fields = [
            "ticket_id",
            "date_submitted",
            "date_under_review",
            "date_completed",
            "dataset_title",
            "dataset_description",
            "agency",
            "purpose_of_request",
            "status",
            "remark",
            "name",
            "email",
            "institution",
            "language",
            "total_subscribers",
            "published_data",
        ]


class AgencySerializer(serializers.ModelSerializer):
    class Meta:
        model = Agency
        fields = ["acronym", "name"]
