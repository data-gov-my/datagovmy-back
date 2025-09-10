from datetime import datetime
from django.core.exceptions import ValidationError

# from data_gov_my.utils.common import LANGUAGE_CHOICES
from django.core.validators import MaxValueValidator
from django.db import models

from data_gov_my.utils.common import SHORT_LANGUAGE_CHOICES


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
        ("academic_publication", "Academic Publication"),
        ("machine_learning", "Machine Learning (ML) product"),
        ("analytics", "Analytics"),
    ]

    # owner details
    name = models.CharField(max_length=255)
    email = models.EmailField()
    institution = models.CharField(max_length=255, blank=True, null=True)

    # product details
    thumbnail = models.ImageField(
        null=True, blank=True, upload_to="community-products/"
    )
    thumbnail_s3 = models.URLField(null=True, blank=True, verbose_name="Thumbnail S3")
    product_name = models.CharField(max_length=255)  # translatable
    product_description = models.TextField()  # translatable
    problem_statement = models.TextField()  # translatable
    solutions_developed = models.TextField()  # translatable

    product_type = models.CharField(
        max_length=20, choices=PRODUCT_TYPE_CHOICES, default="web_application"
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
    date_approved = models.DateTimeField(null=True, blank=True)
    date_rejected = models.DateTimeField(null=True, blank=True)
    remark = models.TextField(blank=True, null=True)  # translatable
    language = models.CharField(
        max_length=2, choices=SHORT_LANGUAGE_CHOICES, default="en"
    )

    def clean(self) -> None:
        errors = dict()
        if self.status == "approved":
            translate_fields = (
                "product_name",
                "product_description",
                "problem_statement",
                "solutions_developed",
            )
            for field in translate_fields:
                if not getattr(self, field + "_ms"):
                    errors[field + "_ms"] = "This field is required."
            if not self.thumbnail_s3:
                errors["thumbnail_s3"] = "This field is required."

        elif self.status == "rejected":
            if not self.remark_en:
                errors["remark_en"] = "This field is required."
            if not self.remark_ms:
                errors["remark_ms"] = "This field is required."

        if errors:
            raise ValidationError(errors)

        return super().clean()

    def __str__(self):
        return f"{self.product_name} ({self.name})"
