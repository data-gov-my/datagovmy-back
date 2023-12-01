import uuid
from django.db import models


# Create your models here.
class DataRequest(models.Model):
    STATUS_CHOICES = [
        ("submitted", "Submitted"),
        ("under_review", "Under Review"),
        ("rejected", "Rejected"),
        ("in_progress", "In Progress"),
        ("data_published", "Data Published"),
    ]

    ticket_id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=255)
    email = models.EmailField()
    institution = models.CharField(max_length=255, blank=True, null=True)
    dataset_title = models.CharField(max_length=255)  # translatable
    dataset_description = models.TextField()  # translatable
    agency = models.CharField(max_length=255)
    purpose_of_request = models.CharField(max_length=255)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="submitted"
    )
