from django.contrib import admin

from data_gov_my.models import (
    ExplorersUpdate,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationUpcoming,
    Subscription,
)
from data_gov_my.models import (
    FormData,
    FormTemplate,
    Publication,
    PublicationResource,
    i18nJson,
)

admin.site.register(i18nJson)
admin.site.register(FormTemplate)
admin.site.register(FormData)
admin.site.register(PublicationResource)
admin.site.register(PublicationDocumentation)
admin.site.register(PublicationDocumentationResource)
admin.site.register(PublicationUpcoming)
admin.site.register(ExplorersUpdate)
admin.site.register(Subscription)


@admin.register(Publication)
class PublicationAdmin(admin.ModelAdmin):
    search_fields = ["publication_id"]
