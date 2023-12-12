from django.contrib import admin
from modeltranslation.admin import TranslationAdmin

from data_request.models import DataRequest


class DataRequestAdmin(TranslationAdmin):
    # list_display = ("ticket_id", "dataset_title", "name", "status")

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        if self.has_change_permission(request):
            # data request manager must update both en and ms fields
            form.base_fields["dataset_title_ms"].required = True
            form.base_fields["dataset_description_ms"].required = True

        return form

    def get_readonly_fields(self, request, obj=None):
        # Specify the fields that should be read-only
        readonly_fields = [
            "ticket_id",
            "name",
            "email",
            "institution",
            "agency",
            "purpose_of_request",
        ]
        return readonly_fields

    def has_change_permission(self, request, obj=None):
        # Allow users in the "Data Request Manager" group to change the object
        return request.user.groups.filter(name="Data Request Manager").exists()


admin.site.register(DataRequest, DataRequestAdmin)
