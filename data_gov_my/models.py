import jsonfield
from django.db import models

class MetaJson(models.Model) :
    dashboard_name = models.CharField(max_length=200)
    dashboard_meta = models.JSONField()

class DashboardJson(models.Model) :
    dashboard_name = models.CharField(max_length=200)
    chart_name = models.CharField(max_length=200, null=True)
    chart_type = models.CharField(max_length=200, null=True)
    api_type = models.CharField(max_length=200, null=True)
    chart_data = models.JSONField()

class CatalogJson(models.Model) :
    id = models.CharField(max_length=400, primary_key=True)
    catalog_meta = models.JSONField()
    catalog_name = models.CharField(max_length=400)
    catalog_category = models.CharField(max_length=300)
    catalog_category_name = models.CharField(max_length=600, default='')
    catalog_subcategory = models.CharField(max_length=300, default='')
    catalog_subcategory_name = models.CharField(max_length=600, default='')
    time_range = models.CharField(max_length=100)
    geographic = models.CharField(max_length=300)
    dataset_begin = models.IntegerField(default=0)
    dataset_end = models.IntegerField(default=0)
    data_source = models.CharField(max_length=100)
    catalog_data = models.JSONField()
    file_src = models.CharField(max_length=400, default='')