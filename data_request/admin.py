from typing import Any

from django import forms
from django.contrib import admin
from django.contrib.admin.widgets import FilteredSelectMultiple
from modeltranslation.admin import TranslationAdmin

from data_catalogue.models import DataCatalogueMeta
from data_request.models import DataRequest


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
        if status in ("under_review", "rejected") and not (remark_en and remark_ms):
            raise forms.ValidationError(
                {
                    "remark_en": "This field is required.",
                    "remark_ms": "This field is required.",
                }
            )
        if status == "data_published" and not (
            published_data.exists()
            or (self.DOCS_SITE_URL in remark_en and self.DOCS_SITE_URL in remark_ms)
        ):
            raise forms.ValidationError(
                {
                    "published_data": 'At least one Data Catalogue must be selected for "data_published" status, else update remark to appropriate link in developer.data.gov.my.',
                    "remark_en": 'This field is required if there is no published data, make sure to include "developer.data.gov.my" in the remark.',
                    "remark_ms": 'This field is required if there is no published data, make sure to include "developer.data.gov.my" in the remark.',
                }
            )
        return cleaned_data


class DataRequestAdmin(TranslationAdmin):
    readonly_fields = [
        "ticket_id",
        "name",
        "email",
        "institution",
        "purpose_of_request",
    ]
    form = DataRequestAdminForm
    list_filter = ["status"]  # Add the 'status' field to enable filtering

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

    def save_model(self, request: Any, obj: Any, form: Any, change: Any) -> None:
        super().save_model(request, obj, form, change)
        obj.published_data.set(form.cleaned_data.get("published_data"))


admin.site.register(DataRequest, DataRequestAdmin)
