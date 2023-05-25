import collections
from django.db import models
from django.contrib.postgres.indexes import GinIndex
from jsonfield import JSONField


class MetaJson(models.Model):
    dashboard_name = models.CharField(max_length=200)
    dashboard_meta = models.JSONField()


class DashboardJson(models.Model):
    dashboard_name = models.CharField(max_length=200)
    chart_name = models.CharField(max_length=200, null=True)
    chart_type = models.CharField(max_length=200, null=True)
    api_type = models.CharField(max_length=200, null=True)
    chart_data = JSONField(load_kwargs={"object_pairs_hook": collections.OrderedDict})


class CatalogJson(models.Model):
    id = models.CharField(max_length=400, primary_key=True)
    catalog_meta = models.JSONField()
    catalog_name = models.CharField(max_length=400)
    catalog_category = models.CharField(max_length=300)
    catalog_category_name = models.CharField(max_length=600, default="")
    catalog_subcategory = models.CharField(max_length=300, default="")
    catalog_subcategory_name = models.CharField(max_length=600, default="")
    time_range = models.CharField(max_length=100)
    geographic = models.CharField(max_length=300)
    demographic = models.CharField(max_length=300)
    dataset_begin = models.IntegerField(default=0)
    dataset_end = models.IntegerField(default=0)
    data_source = models.CharField(max_length=100)
    catalog_data = JSONField(load_kwargs={"object_pairs_hook": collections.OrderedDict})
    file_src = models.CharField(max_length=400, default="")


class NameDashboard_FirstName(models.Model):
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


class NameDashboard_LastName(models.Model):
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
    LANGUAGE_CHOICES = [("en-GB", "English"), ("ms-MY", "Bahasa Melayu")]

    filename = models.CharField(max_length=50)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    route = models.CharField(max_length=100, null=True)  # routes are comma-separated
    translation = models.JSONField()

    def __str__(self) -> str:
        return f"{self.filename} ({self.language})"

    class Meta:
        indexes = [
            models.Index(fields=["filename", "language"], name="filename_language_idx")
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["filename", "language"], name="unique json file by language"
            )
        ]


class ElectionDashboard_Candidates(models.Model):
    name = models.CharField(max_length=100)
    type = models.CharField(max_length=20)
    date = models.CharField(max_length=100)
    election_name = models.CharField(max_length=100)
    seat = models.CharField(max_length=100)
    party = models.CharField(max_length=100)
    votes = models.IntegerField()
    votes_perc = models.FloatField(null=True)
    result = models.CharField(max_length=100)
    voter_turnout = models.IntegerField(null=True)
    voter_turnout_perc = models.FloatField(null=True, default=None)
    votes_rejected = models.IntegerField(null=True)
    votes_rejected_perc = models.FloatField(null=True, default=None)


class ElectionDashboard_Seats(models.Model):
    seat = models.CharField(max_length=100)
    election_name = models.CharField(max_length=100)
    date = models.CharField(max_length=100)
    party = models.CharField(max_length=100)
    name = models.CharField(max_length=100)
    type = models.CharField(max_length=20, default="")
    majority = models.IntegerField()
    majority_perc = models.FloatField(null=True)
    seat_name = models.CharField(max_length=100)
    state = models.CharField(max_length=50, null=True)


class ElectionDashboard_Party(models.Model):
    party = models.CharField(max_length=100)
    type = models.CharField(max_length=20)
    state = models.CharField(max_length=100)
    election_name = models.CharField(max_length=100)
    date = models.CharField(max_length=100)
    seats = models.IntegerField()
    seats_total = models.IntegerField()
    seats_perc = models.FloatField(null=True)
    votes = models.IntegerField()
    votes_perc = models.FloatField(null=True)


class ModsData(models.Model):
    LANGUAGE_CHOICES = [("en-GB", "English"), ("ms-MY", "Bahasa Melayu")]

    expertise_area = models.CharField(max_length=100)
    name = models.CharField(max_length=20)
    email = models.EmailField()
    institution = models.CharField(max_length=50)
    description = models.CharField(max_length=500)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
