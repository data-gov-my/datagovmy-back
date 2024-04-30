import collections

from django.core.validators import validate_email
from django.db import models
from jsonfield import JSONField
from post_office.models import Email, EmailTemplate
from rest_framework.exceptions import ValidationError
from django.contrib.postgres.fields import ArrayField

from data_gov_my.utils.common import LANGUAGE_CHOICES, SITE_CHOICES


class AuthTable(models.Model):
    key = models.CharField(max_length=200, primary_key=True)
    value = models.CharField(max_length=200)
    timestamp = models.DateTimeField()


class MetaJson(models.Model):
    dashboard_name = models.CharField(max_length=200, primary_key=True)
    dashboard_meta = models.JSONField()
    route = models.CharField(max_length=100, null=True)  # routes are comma-separated
    sites = ArrayField(
        models.CharField(max_length=50, choices=SITE_CHOICES),
        default=list,
    )

    def __str__(self) -> str:
        return f"{self.dashboard_name}"


class DashboardJson(models.Model):
    dashboard_name = models.CharField(max_length=200)
    chart_name = models.CharField(max_length=200, null=True)
    chart_type = models.CharField(max_length=200, null=True)
    api_type = models.CharField(max_length=200, null=True)
    chart_data = JSONField(load_kwargs={"object_pairs_hook": collections.OrderedDict})

    def __str__(self) -> str:
        return f"{self.dashboard_name} ({self.chart_name})"


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
    filename = models.CharField(max_length=50)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    route = models.CharField(max_length=100, null=True)  # routes are comma-separated
    sites = ArrayField(
        models.CharField(max_length=50, choices=SITE_CHOICES),
        default=list,
    )
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


class FormTemplate(models.Model):
    form_type = models.CharField(max_length=50, primary_key=True)
    email_template = models.ForeignKey(EmailTemplate, on_delete=models.CASCADE)
    form_meta = models.JSONField()

    def __str__(self) -> str:
        return f"{self.form_type}"

    @classmethod
    def create(cls, form_type: str, form_meta: dict):
        form_template = cls(form_type=form_type, form_meta=form_meta)
        default_template = cls.get_or_create_email_template(
            form_meta.get("email_template", [])
        )
        form_template.email_template = default_template
        form_template.save()
        return form_template

    @staticmethod
    def get_or_create_email_template(email_template_meta):
        """
        Helper function to get or create the email template object based on form meta
        """
        # first email template in meta is used as the default template
        meta = email_template_meta[0]

        # EmailTemplate.objects.filter(
        #     name=meta["name"], language=meta["language"]
        # ).delete()

        default_template, _ = EmailTemplate.objects.update_or_create(
            name=meta["name"],
            language=meta["language"],
            defaults={
                "subject": meta["subject"],
                "content": meta["content"],
                "html_content": meta["html_content"],
            },
        )

        for i in range(1, len(email_template_meta)):
            meta = email_template_meta[i]
            alt_template, _ = default_template.translated_templates.update_or_create(
                language=meta["language"],
                defaults={
                    "subject": meta["subject"],
                    "content": meta["content"],
                    "html_content": meta["html_content"],
                },
            )

        return default_template

    def can_send_email(self):
        return self.form_meta.get("send_email", False)

    def validate_form(self, payload: dict):
        """
        Validates form data based on the specified fields in form_meta.
        """
        # validate field names
        validate_fields = self.form_meta.get("validate_fields", [])
        if not collections.Counter(validate_fields) == collections.Counter(
            payload.keys()
        ):
            raise ValidationError(detail={"Fields required": validate_fields})
        if "email" in payload:
            try:
                validate_email(payload["email"])
            except Exception as e:
                raise ValidationError(detail={"email": "Enter a valid email address."})
        return True

    def create_form_data(self, payload: dict):
        """
        Validates the form data (payload), then create and store a new FormData instance
        """
        if self.validate_form(payload):
            lang = payload.get("language", "en-GB")
            form_data = self.formdata_set.create(
                language=lang,
                form_data=payload,
            )
            return form_data


class FormData(models.Model):
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    form_data = models.JSONField()
    form_type = models.ForeignKey(FormTemplate, on_delete=models.CASCADE)
    email = models.OneToOneField(Email, on_delete=models.CASCADE, null=True)

    def __str__(self) -> str:
        return f"{self.form_type} | {self.language} ({self.id})"

    def get_recipient(self) -> str:
        return self.form_data.get("email", None)


class ExplorersMetaJson(MetaJson):
    pass


class ExplorersUpdate(models.Model):
    explorer = models.CharField(max_length=100, null=False)
    file_name = models.CharField(max_length=100, null=False)
    last_update = models.CharField(max_length=100, null=False)

    def __str__(self) -> str:
        return f"{self.explorer} ({self.file_name})"


class Publication(models.Model):
    publication_id = models.CharField(max_length=30)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    publication_type = models.CharField(max_length=50)
    publication_type_title = models.CharField(max_length=150)
    title = models.CharField(max_length=150)
    description = models.CharField(max_length=300)
    release_date = models.DateField()
    frequency = models.CharField(max_length=50)
    geography = ArrayField(
        models.CharField(max_length=10, blank=True),
    )
    demography = ArrayField(
        models.CharField(max_length=10, blank=True),
    )

    class Meta:
        indexes = [
            models.Index(
                fields=["publication_id", "language"],
                name="publication_id_language_idx",
            )
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["publication_id", "language"],
                name="unique publication by language",
            )
        ]

    def __str__(self) -> str:
        return f"{self.publication_id} ({self.language})"


class PublicationSubscription(models.Model):
    publication_type = models.CharField(max_length=50, primary_key=True)
    emails = ArrayField(models.EmailField(), default=list)


class PublicationResource(models.Model):
    resource_id = models.IntegerField()
    resource_type = models.CharField(max_length=50)
    resource_name = models.CharField(max_length=100)
    resource_link = models.URLField(max_length=150)
    downloads = models.PositiveIntegerField(default=0)
    publication = models.ForeignKey(
        Publication, related_name="resources", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["resource_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["resource_id", "publication"],
                name="unique publication resource by publication and id",
            )
        ]

    def __str__(self) -> str:
        return f"{self.publication} - {self.resource_name}"


class PublicationDocumentation(models.Model):
    publication_id = models.CharField(max_length=30)
    documentation_type = models.CharField(max_length=30)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    publication_type = models.CharField(max_length=100)
    publication_type_title = models.CharField(max_length=150)
    title = models.CharField(max_length=100)
    description = models.CharField(max_length=300)
    release_date = models.DateField()

    class Meta:
        indexes = [
            models.Index(
                fields=["publication_id", "language"],
                name="pub_docs_id_language_idx",
            ),
            models.Index(
                fields=["documentation_type", "language"],
                name="pub_docs_type_language_idx",
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["publication_id", "language"],
                name="unique publication documentation by language",
            )
        ]

    def __str__(self) -> str:
        return f"{self.publication_id} ({self.language})"


class PublicationDocumentationResource(models.Model):
    resource_id = models.IntegerField()
    resource_type = models.CharField(max_length=100)
    resource_name = models.CharField(max_length=150)
    resource_link = models.URLField(max_length=150)
    downloads = models.PositiveIntegerField(default=0)
    publication = models.ForeignKey(
        PublicationDocumentation, related_name="resources", on_delete=models.CASCADE
    )

    class Meta:
        ordering = ["resource_id"]
        constraints = [
            models.UniqueConstraint(
                fields=["resource_id", "publication"],
                name="unique publication documentation resource by publication and id",
            )
        ]

    def __str__(self) -> str:
        return f"{self.publication} - {self.resource_name}"


class PublicationUpcoming(models.Model):
    publication_id = models.CharField(max_length=100)
    language = models.CharField(max_length=5, choices=LANGUAGE_CHOICES, default="en-GB")
    publication_type = models.CharField(max_length=100)
    publication_type_title = models.CharField(max_length=150)
    release_date = models.DateField()
    publication_title = models.CharField(max_length=150)
    product_type = models.CharField(max_length=150)
    release_series = models.CharField(max_length=150)

    class Meta:
        ordering = ["release_date", "publication_id"]
        indexes = [
            models.Index(
                fields=["language"],
                name="upcoming_pub_language_idx",
            )
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["publication_id", "language"],
                name="unique upcoming publication by id and language",
            )
        ]

    def __str__(self) -> str:
        return f"{self.publication_id} ({self.language})"


class KTMBTimeseries(models.Model):
    service = models.CharField(max_length=100)
    origin = models.CharField(max_length=100)
    destination = models.CharField(max_length=100)
    date = models.DateField()
    passengers = models.IntegerField(null=True)
    frequency = models.CharField(max_length=50)

    class Meta:
        indexes = [
            models.Index(
                fields=["service", "origin", "destination"],
                name="ktmb_timeseries_idx",
            )
        ]


class KTMBTimeseriesCallout(models.Model):
    service = models.CharField(max_length=100)
    origin = models.CharField(max_length=100)
    destination = models.CharField(max_length=100)
    passengers = models.IntegerField(null=True)
    frequency = models.CharField(max_length=50)

    class Meta:
        indexes = [
            models.Index(
                fields=["service", "origin", "destination"],
                name="ktmb_callout_idx",
            )
        ]


class PrasaranaTimeseries(models.Model):
    service = models.CharField(max_length=100)
    origin = models.CharField(max_length=100)
    destination = models.CharField(max_length=100)
    date = models.DateField()
    passengers = models.IntegerField(null=True)
    frequency = models.CharField(max_length=50)

    class Meta:
        indexes = [
            models.Index(
                fields=["service", "origin", "destination"],
                name="prasarana_timeseries_idx",
            )
        ]


class PrasaranaTimeseriesCallout(models.Model):
    service = models.CharField(max_length=100)
    origin = models.CharField(max_length=100)
    destination = models.CharField(max_length=100)
    passengers = models.IntegerField(null=True)
    frequency = models.CharField(max_length=50)

    class Meta:
        indexes = [
            models.Index(
                fields=["service", "origin", "destination"],
                name="prasarana_callout_idx",
            )
        ]
