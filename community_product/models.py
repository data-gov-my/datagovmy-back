from datetime import datetime

# from data_gov_my.utils.common import LANGUAGE_CHOICES
from django.core.validators import MaxValueValidator
from django.db import models


# Create your models here.
def get_current_year():
    return datetime.now().year


class CommunityProduct(models.Model):
    STATUS_CHOICES = [
        ("submitted", "Submitted"),
        ("approved", "Approved"),
        ("rejected", "Rejected"),
    ]

    PRODUCT_TYPE_CHOICES = [
        ("web_application", "Web application"),
        ("mobile_application", "Mobile application"),
        ("dashboard", "Dashboard"),
        ("academic", "Academic work"),
        ("machine_learning", "Machine Learning (ML) product"),
        ("analytics", "Analytics"),
        ("publications", "Publications"),
    ]

    # owner details
    name = models.CharField(max_length=255)
    email = models.EmailField()
    institution = models.CharField(max_length=255, blank=True, null=True)

    # product details
    product_name = models.CharField(max_length=255)
    product_description = models.TextField()
    product_type = models.CharField(
        max_length=18, choices=PRODUCT_TYPE_CHOICES, default="web_application"
    )
    product_year = models.PositiveIntegerField(
        default=get_current_year, validators=[MaxValueValidator(3000)]
    )
    product_link = models.URLField()
    dataset_used = models.TextField()  # FIXME:TextField?

    # ticket detail
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="submitted"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    # language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")

    # TODO: add image field

    def __str__(self):
        return f"{self.product_name} ({self.name})"
