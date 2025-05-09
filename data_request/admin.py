import os
from typing import Any

from django import forms
from django.contrib import admin
from django.contrib.admin.widgets import FilteredSelectMultiple
from django.utils import timezone, translation
from django_better_admin_arrayfield.admin.mixins import DynamicArrayMixin
from modeltranslation.admin import TranslationAdmin, TranslationTabularInline
from post_office import mail

from data_catalogue.models import DataCatalogueMeta
from data_request.models import Agency, DataRequest
from data_request.serializers import DataRequestSerializer


class DataRequestAdminForm(forms.ModelForm):
    published_data = forms.ModelMultipleChoiceField(
        queryset=DataCatalogueMeta.objects.all(),
        widget=FilteredSelectMultiple("Data Catalogue Items", False),
        required=False,
    )
    DOCS_SITE_URL = "developer.data.gov.my"

    class Meta:
        model = DataRequest
        fields = "__all__"

    def clean(self) -> dict[str, Any]:
        cleaned_data = super().clean()
        status = cleaned_data.get("status")
        published_data = cleaned_data.get("published_data")
        remark_en: str = cleaned_data.get("remark_en") or ""
        remark_ms: str = cleaned_data.get("remark_ms") or ""
        if status in ("under_review", "rejected"):
            errors = {}
            if not remark_en:
                errors["remark_en"] = "This field is required."
            if not remark_ms:
                errors["remark_ms"] = "This field is required."
            if errors:
                raise forms.ValidationError(errors)
        if status == "data_published" and not (
                published_data.exists()
                or (self.DOCS_SITE_URL in remark_en and self.DOCS_SITE_URL in remark_ms)
        ):
            raise forms.ValidationError(
                {
                    "published_data": 'At least one Data Catalogue must be selected for "data_published" status, else update remark to appropriate link in developer.data.gov.my.',
                    "remark_en": 'This field is required if there is no published data, include "developer.data.gov.my" in the remark.',
                    "remark_ms": 'This field is required if there is no published data, include "developer.data.gov.my" in the remark.',
                }
            )
        return cleaned_data


class DataRequestAdmin(TranslationAdmin):
    readonly_fields = [
        "ticket_id",
        "purpose_of_request",
        "date_submitted",
        "date_under_review",
        "date_completed",
    ]
    form = DataRequestAdminForm
    list_filter = ["status"]  # Add the 'status' field to enable filtering
    DATA_REQUEST_UNDER_REVIEW_TEMPLATE = "data_request_under_review"
    DATA_REQUEST_AGENCY_NOTIFICATION_TEMPLATE = "data_request_agency_notification"
    DATA_REQUEST_DATA_PUBLISHED_TEMPLATE = "data_request_data_published"
    DATA_REQUEST_REJECTED_TEMPLATE = "data_request_rejected"

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if obj and obj.pk:
            form.base_fields["published_data"].initial = obj.published_data.values_list(
                "pk", flat=True
            )
        # data request manager must update both en and ms fields
        form.base_fields["dataset_title_ms"].required = True
        form.base_fields["dataset_description_ms"].required = True

        return form

    def send_subscribtion_emails(self, obj: DataRequest, template: str, context={}):
        with translation.override("en"):
            recipients = obj.subscription_set.filter(language="en-GB").values_list(
                "email", flat=True
            )
            email_context = DataRequestSerializer(obj).data
            email_context.update(context)
            if recipients.exists():
                mail.send(
                    sender=os.getenv('DEFAULT_FROM_EMAIL_DATA_REQUEST'),
                    bcc=list(recipients),
                    template=template,
                    language="en-GB",
                    context=email_context,
                    backend="data_request",
                )

        with translation.override("ms"):
            recipients = obj.subscription_set.filter(language="ms-MY").values_list(
                "email", flat=True
            )
            email_context = DataRequestSerializer(obj).data
            email_context.update(context)
            if recipients.exists():
                mail.send(
                    sender=os.getenv('DEFAULT_FROM_EMAIL_DATA_REQUEST'),
                    bcc=list(recipients),
                    template=template,
                    language="ms-MY",
                    context=email_context,
                    backend="data_request",
                )

    def save_model(self, request: Any, obj: Any, form: Any, change: Any) -> None:
        obj.published_data.set(form.cleaned_data.get("published_data"))
        if obj.status == "under_review" and not obj.date_under_review:
            obj.date_under_review = timezone.now()
            # send email updating user that it is now under review
            self.send_subscribtion_emails(
                obj=obj,
                template=self.DATA_REQUEST_UNDER_REVIEW_TEMPLATE,
                context={"name": obj.subscription_set.first().name},
            )
            # send email to agency to request for review
            with translation.override("ms"):
                context = DataRequestSerializer(obj).data
                mail.send(
                    sender=os.getenv('DEFAULT_FROM_EMAIL_DATA_REQUEST'),
                    recipients=obj.agency.emails,
                    template=self.DATA_REQUEST_AGENCY_NOTIFICATION_TEMPLATE,
                    language="ms",
                    context=context,
                    backend="data_request",
                )
        elif obj.status in ["rejected", "data_published"]:
            obj.date_completed = timezone.now()
        super().save_model(request, obj, form, change)

        # after model is saved, make sure to send relevant emails
        if obj.status == "rejected":
            self.send_subscribtion_emails(
                obj=obj, template=self.DATA_REQUEST_REJECTED_TEMPLATE
            )
        if obj.status == "data_published":
            self.send_subscribtion_emails(
                obj=obj, template=self.DATA_REQUEST_DATA_PUBLISHED_TEMPLATE
            )


class AgencyInline(TranslationTabularInline):
    model = Agency


class AgencyAdmin(TranslationAdmin, DynamicArrayMixin):
    list_display = ["acronym", "name_en", "name_ms"]


admin.site.register(DataRequest, DataRequestAdmin)
admin.site.register(Agency, AgencyAdmin)
