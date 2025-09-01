from django.db import models
from django_better_admin_arrayfield.models.fields import ArrayField

from data_gov_my.utils.common import LANGUAGE_CHOICES

# Create your models here.


class Agency(models.Model):
    acronym = models.CharField(max_length=15, primary_key=True)
    name = models.CharField(max_length=255)  # translatable
    emails = ArrayField(models.EmailField())

    def __str__(self) -> str:
        return self.acronym

    class Meta:
        verbose_name_plural = "agencies"

class DataRequestAdminEmail(models.Model):
    email = models.EmailField(unique=True, help_text="Email address of the data request administrator")
    added_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        verbose_name = "Data Request Admin Email"
        verbose_name_plural = "Data Request Admin Emails"

class DataRequest(models.Model):
    STATUS_CHOICES = [
        ("submitted", "Submitted"),
        ("under_review", "Under Review"),
        ("rejected", "Rejected"),
        ("data_published", "Data Published"),
    ]
    ticket_id = models.AutoField(primary_key=True, editable=False)

    dataset_title = models.CharField(max_length=255)  # translatable
    dataset_description = models.TextField()  # translatable
    agency = models.ForeignKey(Agency, null=True, on_delete=models.SET_NULL)
    purpose_of_request = models.CharField(max_length=255)
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="submitted"
    )
    remark = models.TextField(blank=True, null=True)  # translatable
    live_remark = models.TextField(blank=True, null=True)
    # keep track of dates
    date_submitted = models.DateTimeField(auto_now_add=True)
    date_under_review = models.DateTimeField(null=True, blank=True)
    date_completed = models.DateTimeField(null=True, blank=True)
    published_data = models.ManyToManyField(
        "data_catalogue.DataCatalogueMeta", blank=True
    )

    def __str__(self) -> str:
        return f"{self.ticket_id} ({self.dataset_title})"


class Subscription(models.Model):
    name = models.CharField(max_length=255)
    email = models.EmailField()
    institution = models.CharField(max_length=255, blank=True, null=True)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    # FIXME disable null=True after migration completed
    data_request = models.ForeignKey(DataRequest, on_delete=models.CASCADE, null=True)

    def __str__(self) -> str:
        return f"{self.name} ({self.email}))"

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["email", "data_request"], name="unique_subscription"
            )
        ]
