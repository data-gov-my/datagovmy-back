from rest_framework import serializers

from data_gov_my.models import (
    CatalogJson,
    DashboardJson,
    FormData,
    MetaJson,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationUpcoming,
    ViewCount,
    Publication,
    PublicationResource,
    i18nJson,
)


class MetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = MetaJson
        fields = ["dashboard_name", "dashboard_meta"]


class DashboardSerializer(serializers.ModelSerializer):
    class Dashboard:
        model = DashboardJson
        fields = ["dashboard_name", "chart_name", "chart_type", "chart_data"]


class CatalogSerializer(serializers.ModelSerializer):
    class Catalog:
        model = CatalogJson
        fields = [
            "id",
            "catalog_meta",
            "catalog_name",
            "catalog_category",
            "time_range",
            "geography",
            "dataset_range",
            "data_source",
            "catalog_data",
        ]


class i18nSerializer(serializers.ModelSerializer):
    class Meta:
        model = i18nJson
        exclude = ["id"]

    def update(self, instance, validated_data):
        i18n_object = i18nJson.objects.filter(
            filename=instance.filename, language=instance.language
        ).update(**validated_data)
        return i18n_object


class FormDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = FormData
        fields = ["language", "form_data"]


class ViewCountSerializer(serializers.ModelSerializer):
    class Meta:
        model = ViewCount
        fields = "__all__"


class ViewCountPartialSerializer(serializers.ModelSerializer):
    """
    Only returns the `view_count` field for metrics based fields.
    """

    class Meta:
        model = ViewCount
        exclude = ["download_csv", "download_parquet", "download_png", "download_svg"]


class ViewCountSerializerV2(serializers.ModelSerializer):
    class Meta:
        model = ViewCount
        fields = "__all__"

    def to_representation(self, instance):
        data = super().to_representation(instance)
        return dict(data)


class PublicationResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PublicationResource
        exclude = ["id", "publication"]


class PublicationDetailSerializer(serializers.ModelSerializer):
    resources = PublicationResourceSerializer(many=True)

    class Meta:
        model = Publication
        fields = ["title", "description", "release_date", "resources"]


class PublicationSerializer(serializers.ModelSerializer):
    total_downloads = serializers.IntegerField()

    class Meta:
        model = Publication
        fields = [
            "publication_id",
            "publication_type",
            "title",
            "description",
            "release_date",
            "total_downloads",
        ]


class PublicationDocumentationSerializer(serializers.ModelSerializer):
    total_downloads = serializers.IntegerField()

    class Meta:
        model = PublicationDocumentation
        fields = [
            "publication_id",
            "publication_type",
            "publication_type_title",
            "title",
            "description",
            "release_date",
            "total_downloads",
        ]


class PublicationDocumentationResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = PublicationDocumentationResource
        fields = "__all__"


class PublicationDocumentationDetailSerializer(serializers.ModelSerializer):
    resources = PublicationDocumentationResourceSerializer(many=True)

    class Meta:
        model = Publication
        fields = ["title", "description", "release_date", "resources"]


class PublicationUpcomingSerializer(serializers.ModelSerializer):
    class Meta:
        model = PublicationUpcoming
        exclude = ["id", "language"]
