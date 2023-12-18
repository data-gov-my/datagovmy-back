from django.contrib import admin
from modeltranslation.admin import TranslationAdmin
from data_catalogue.models import *


# Custom admin classes for translated models


# Register your models with the custom admin classes
admin.site.register(DataCatalogue)
admin.site.register(DataCatalogueMeta, TranslationAdmin)
admin.site.register(SiteCategory, TranslationAdmin)
admin.site.register(Field, TranslationAdmin)
admin.site.register(RelatedDataset, TranslationAdmin)
admin.site.register(Dataviz, TranslationAdmin)
