import jsonfield
from django.db import models
from django.contrib.postgres.indexes import GinIndex


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

class NameDashboard_FirstName(models.Model) :
    name = models.CharField(max_length=30, primary_key=True)
    d_1920 = models.IntegerField(null=True, default=0)
    d_1930 = models.IntegerField(null=True, default=0)
    d_1940 = models.IntegerField(null=True, default=0)
    d_1950 = models.IntegerField(null=True, default=0)
    d_1960 = models.IntegerField(null=True, default=0)
    d_1970 = models.IntegerField(null=True, default=0)
    d_1980 = models.IntegerField(null=True, default=0)
    d_1990 = models.IntegerField(null=True, default=0)
    d_2000 = models.IntegerField(null=True, default=0)
    d_2010 = models.IntegerField(null=True, default=0)
    total = models.IntegerField(null=True, default=0)

class NameDashboard_LastName(models.Model) :
    name = models.CharField(max_length=30, primary_key=True)
    d_1920 = models.IntegerField(null=True, default=0)
    d_1930 = models.IntegerField(null=True, default=0)
    d_1940 = models.IntegerField(null=True, default=0)
    d_1950 = models.IntegerField(null=True, default=0)
    d_1960 = models.IntegerField(null=True, default=0)
    d_1970 = models.IntegerField(null=True, default=0)
    d_1980 = models.IntegerField(null=True, default=0)
    d_1990 = models.IntegerField(null=True, default=0)
    d_2000 = models.IntegerField(null=True, default=0)
    d_2010 = models.IntegerField(null=True, default=0)
    total = models.IntegerField(null=True, default=0)


class i18nJson(models.Model):
    LANGUAGE_CHOICES = [('en', 'English'), ('bm', 'Bahasa Melayu')]

    filename = models.CharField(max_length=50)
    language = models.CharField(max_length=2, choices=LANGUAGE_CHOICES, default='en')
    route = models.CharField(max_length=50)
    translation_json = models.JSONField()

    class Meta:
        indexes = [
            models.Index(fields=["filename", "language"], name="filename_language_idx")
        ]
        constraints = [
            models.UniqueConstraint(fields=["filename", "language"], name="unique json file by language")
        ]