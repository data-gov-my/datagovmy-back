from django.contrib import admin
from data_gov_my.models import (
    CatalogJson,
    ExplorersUpdate,
    FormData,
    FormTemplate,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationUpcoming,
    ViewCount,
    i18nJson,
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
admin.site.register(ViewCount)
admin.site.register(Publication)
admin.site.register(PublicationResource)
admin.site.register(PublicationDocumentation)
admin.site.register(PublicationDocumentationResource)
admin.site.register(PublicationUpcoming)
admin.site.register(CatalogJson)
admin.site.register(ExplorersUpdate)
