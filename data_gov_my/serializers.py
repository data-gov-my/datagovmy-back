from rest_framework import serializers
from .models import MetaJson, DashboardJson, CatalogJson, NameDashboard_FirstName, i18nJson

class MetaSerializer(serializers.ModelSerializer) :
    class Meta : 
        model = MetaJson
        fields = ['dashboard_name', 'dashboard_meta']

class DashboardSerializer(serializers.ModelSerializer) :
    class Dashboard : 
        model = DashboardJson
        fields = ['dashboard_name', 'chart_name', 'chart_type', 'chart_data']

class CatalogSerializer(serializers.ModelSerializer) :
    class Catalog : 
        model = CatalogJson
        fields = ["id", "catalog_meta", "catalog_name", "catalog_category", "time_range", "geographic", "dataset_range", "data_source", "catalog_data"]


class i18nSerializer(serializers.ModelSerializer):
    class Meta:
        model = i18nJson 
        fields = '__all__'

    def update(self, instance, validated_data):
        i18n_object = i18nJson.objects.filter(filename=instance.filename).update(**validated_data)
        return i18n_object