from django.db import models

# Create your models here.

from django.db import models
from django.contrib.postgres.indexes import GinIndex


class Address(models.Model):
    address = models.TextField()
    district = models.CharField(max_length=100)
    subdistrict = models.CharField(max_length=100)
    locality = models.CharField(max_length=255)
    parlimen = models.CharField(max_length=100)
    dun = models.CharField(max_length=100)
    lat = models.DecimalField(max_digits=10, decimal_places=7)
    lon = models.DecimalField(max_digits=10, decimal_places=7)
    postcode = models.CharField(max_length=8)

    def __str__(self):
        return self.address

    class Meta:
        indexes = [
            models.Index(fields=["postcode"]),
            GinIndex(fields=["address"]),
        ]
