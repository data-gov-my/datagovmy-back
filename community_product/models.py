from django.db import models

# from data_gov_my.utils.common import LANGUAGE_CHOICES

# Create your models here.


class CommunityProduct(models.Model):
    STATUS_CHOICES = [
        ("submitted", "Submitted"),
        ("approved", "Approved"),
        ("rejected", "Rejected"),
    ]

    PRODUCT_TYPE_CHOICES = [
        ("web", "Web app"),
        ("mobile", "Mobile app"),
        ("academic", "Academic work"),
        ("ai_ml", "Artificial Intelligence/Machine Learning"),
    ]

    # owner details
    name = models.CharField(max_length=255)
    email = models.EmailField()
    institution = models.CharField(max_length=255, blank=True, null=True)
    # product details
    product_name = models.CharField(max_length=255)
    product_description = models.TextField()
    status = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default="submitted"
    )
    product_link = models.URLField()
    dataset_used = models.TextField()  # FIXME:TextField?
    created_at = models.DateTimeField(auto_now_add=True)
    # language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")

    # TODO: add image field

    def __str__(self):
        return f"{self.product_name} ({self.name})"
