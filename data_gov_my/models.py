import collections

from django.core.validators import validate_email
from django.db import models
from jsonfield import JSONField
from post_office.models import Email, EmailTemplate
from rest_framework.exceptions import ValidationError

from data_gov_my.utils.common import LANGUAGE_CHOICES


class MetaJson(models.Model):
    dashboard_name = models.CharField(max_length=200, primary_key=True)
    dashboard_meta = models.JSONField()
    route = models.CharField(max_length=100, null=True)  # routes are comma-separated

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


class CatalogJson(models.Model):
    id = models.CharField(max_length=400, primary_key=True)
    catalog_meta = models.JSONField()
    catalog_name = models.CharField(max_length=400)
    catalog_category = models.CharField(max_length=300)
    catalog_category_name = models.CharField(max_length=600, default="")
    catalog_subcategory = models.CharField(max_length=300, default="")
    catalog_subcategory_name = models.CharField(max_length=600, default="")
    time_range = models.CharField(max_length=100)
    geography = models.CharField(max_length=300)
    demography = models.CharField(max_length=300)
    dataset_begin = models.IntegerField(default=0)
    dataset_end = models.IntegerField(default=0)
    data_source = models.CharField(max_length=100)
    catalog_data = JSONField(load_kwargs={"object_pairs_hook": collections.OrderedDict})
    file_src = models.CharField(max_length=400, default="")

    def __str__(self) -> str:
        return f"{self.file_src} - {self.catalog_name}"


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
    majority = models.IntegerField(default=0)
    majority_perc = models.FloatField(null=True, default=None)
    slug = models.CharField(max_length=100, default="")


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
    slug = models.CharField(max_length=100, default="")
    voter_turnout = models.IntegerField(null=True)
    voter_turnout_perc = models.FloatField(null=True, default=None)
    votes_rejected = models.IntegerField(null=True)
    votes_rejected_perc = models.FloatField(null=True, default=None)


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


class ElectionDashboard_Dropdown(models.Model):
    state = models.CharField(max_length=100, default="")
    election = models.CharField(max_length=50, default="")
    date = models.IntegerField(default=0)


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

        default_template: EmailTemplate = EmailTemplate.objects.get_or_create(
            name=meta["name"],
            subject=meta["subject"],
            content=meta["content"],
            html_content=meta["html_content"],
            language=meta["language"],
        )[0]

        for i in range(1, len(email_template_meta)):
            meta = email_template_meta[i]
            alt_template = default_template.translated_templates.get_or_create(
                subject=meta["subject"],
                content=meta["content"],
                html_content=meta["html_content"],
                language=meta["language"],
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


class ViewCount(models.Model):
    id = models.CharField(max_length=100, primary_key=True)
    type = models.CharField(max_length=100, null=False)
    all_time_view = models.IntegerField(null=False, default=0)
    download_csv = models.IntegerField(null=False, default=0)
    download_parquet = models.IntegerField(null=False, default=0)
    download_png = models.IntegerField(null=False, default=0)
    download_svg = models.IntegerField(null=False, default=0)

    def __str__(self) -> str:
        return f"{self.id} ({self.all_time_view})"


class ExplorersUpdate(models.Model):
    explorer = models.CharField(max_length=100, null=False)
    file_name = models.CharField(max_length=100, null=False)
    last_update = models.CharField(max_length=100, null=False)
