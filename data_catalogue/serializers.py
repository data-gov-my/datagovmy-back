from rest_framework import serializers

from data_catalogue.models import (
    DataCatalogue,
    DataCatalogueMeta,
    Dataviz,
    Field,
    RelatedDataset,
)


class FieldSerializer(serializers.ModelSerializer):
    class Meta:
        model = Field
        fields = ["name", "title", "description"]


class RelatedDatasetSerializer(serializers.ModelSerializer):
    class Meta:
        model = RelatedDataset
        fields = ["id", "title", "description"]


class DataCatalogueSerializer(serializers.ModelSerializer):
    class Meta:
        model = DataCatalogue
        fields = ["data"]


class DatavizSerializer(serializers.ModelSerializer):
    class Meta:
        model = Dataviz
        fields = ["dataviz_id", "title", "chart_type", "config"]


class DataCatalogueMetaSerializer(serializers.ModelSerializer):
    fields = FieldSerializer(many=True)
    related_datasets = RelatedDatasetSerializer(many=True)
    dataviz_set = DatavizSerializer(many=True)
    # data = serializers.SerializerMethodField()

    class Meta:
        model = DataCatalogueMeta
        fields = [
            "id",
            "exclude_openapi",
            "manual_trigger",
            "title",
            "description",
            "link_parquet",
            "link_csv",
            "link_preview",
            # "frequency",
            # "geography",
            # "demography",
            # "dataset_begin",
            # "dataset_end",
            # "data_source",
            "fields",
            "data_as_of",
            "last_updated",
            "next_update",
            "methodology",
            "caveat",
            "publication",
            "translations",
            "related_datasets",
            # "data",
            "dataviz_set",
        ]
