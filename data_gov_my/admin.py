from django.contrib import admin

from data_gov_my.models import (
    ExplorersUpdate,
    PublicationDocumentation,
    PublicationDocumentationResource,
    PublicationUpcoming,
    Subscription, PublicationType, PublicationSubtype,
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


@admin.register(Subscription)
class SubscriptionAdmin(admin.ModelAdmin):
    list_display = ['created_at', 'email', 'publications', 'language']


@admin.register(Publication)
class PublicationAdmin(admin.ModelAdmin):
    search_fields = ["publication_id"]


@admin.register(PublicationType)
class PublicationTypeAdmin(admin.ModelAdmin):
    list_display = ['order', 'id', 'type_en', 'type_bm']
    ordering = ('order',)

@admin.register(PublicationSubtype)
class PublicationSubtypeAdmin(admin.ModelAdmin):
    list_display = ['publication_type', 'order', 'id', 'subtype_en', 'subtype_bm']
    ordering = ('publication_type__order', 'order')