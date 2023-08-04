from django.contrib import admin
from data_gov_my.models import FormData, FormTemplate, ViewCount, i18nJson

admin.site.register(i18nJson)
admin.site.register(FormTemplate)
admin.site.register(FormData)
admin.site.register(ViewCount)
