from django.db import models

# Create your models here.

from django.db import models


class Address(models.Model):
    unit = models.CharField(max_length=255, null=True)
    namaBangunan = models.CharField(max_length=255, null=True)
    poBox = models.CharField(null=True, blank=True, max_length=255)
    namaJalan = models.CharField(max_length=255)
    lokaliti = models.CharField(max_length=255)
    poskod = models.CharField(max_length=5)
    bandar = models.CharField(max_length=255)
    negeri = models.CharField(max_length=255)
    negara = models.CharField(max_length=255)
    latitud1 = models.FloatField()
    longitud1 = models.FloatField()
    postcode = models.CharField(max_length=8)

    class Meta:
        indexes = [
            models.Index(fields=["postcode"]),
        ]
