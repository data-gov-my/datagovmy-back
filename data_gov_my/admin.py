from django.contrib import admin
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _

# imported explicitly so post_office's EmailTemplate admin is registered before we replace it
import post_office.admin as post_office_admin
from post_office.models import EmailTemplate

from data_gov_my.utils.common import LANGUAGE_CHOICES, SHORT_LANGUAGE_CHOICES
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


# post_office's EmailTemplate admin limits translation languages to settings.LANGUAGES ("en", "ms"),
# but data request templates are sent with "en-GB" / "ms-MY". Saving a template with such a
# translation failed language validation, and the formset clean() then raised a KeyError (HTTP 500).
EMAIL_TEMPLATE_LANGUAGE_CHOICES = [
    (code, f"{label} ({code})") for code, label in LANGUAGE_CHOICES + SHORT_LANGUAGE_CHOICES
]


class EmailTemplateAdminForm(post_office_admin.EmailTemplateAdminForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        choices = list(EMAIL_TEMPLATE_LANGUAGE_CHOICES)
        if self.instance.language and self.instance.language not in dict(choices):
            choices.append((self.instance.language, self.instance.language))
        self.fields["language"].choices = choices


class EmailTemplateAdminFormSet(post_office_admin.EmailTemplateAdminFormSet):
    def clean(self):
        """
        Check that no two Email templates have the same default_template and language,
        skipping blank, deleted or invalid forms instead of raising a KeyError.
        """
        super(post_office_admin.EmailTemplateAdminFormSet, self).clean()
        data = set()
        for form in self.forms:
            if self.can_delete and self._should_delete_form(form):
                continue
            default_template = form.cleaned_data.get("default_template")
            language = form.cleaned_data.get("language")
            if default_template is None or language is None:
                continue
            if (default_template.id, language) in data:
                msg = _("Duplicate template for language '{language}'.")
                language = dict(form.fields["language"].choices)[language]
                raise ValidationError(msg.format(language=language))
            data.add((default_template.id, language))


class EmailTemplateInline(post_office_admin.EmailTemplateInline):
    form = EmailTemplateAdminForm
    formset = EmailTemplateAdminFormSet

    def get_max_num(self, request, obj=None, **kwargs):
        return len(EMAIL_TEMPLATE_LANGUAGE_CHOICES)


class EmailTemplateAdmin(post_office_admin.EmailTemplateAdmin):
    form = EmailTemplateAdminForm
    inlines = (EmailTemplateInline,)


admin.site.unregister(EmailTemplate)
admin.site.register(EmailTemplate, EmailTemplateAdmin)
