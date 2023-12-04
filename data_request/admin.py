from django.contrib import admin

from data_request.models import DataRequest
from modeltranslation.admin import TranslationAdmin


class DataRequestAdmin(TranslationAdmin):
    pass


admin.site.register(DataRequest, DataRequestAdmin)
