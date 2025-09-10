import logging

from django.conf import settings
from django.contrib import admin
from django.utils import translation, timezone
from modeltranslation.admin import TranslationAdmin
from post_office import mail
from .models import CommunityProduct

# Register your models here.


class CommunityProductAdmin(TranslationAdmin):
    readonly_fields = [
        "name",
        "email",
        "institution",
        "created_at",
        "product_link",
        "product_year",
        "date_approved",
        "date_rejected",
        "language",
    ]
    list_filter = ["product_type", "status"]
    exclude = ["thumbnail"]
    COMMUNITY_PRODUCT_APPROVED_TEMPLATE = "community_product_approved"
    COMMUNITY_PRODUCT_REJECTED_TEMPLATE = "community_product_rejected"

    # def get_form(self, request, obj=None, **kwargs):
    #     form = super().get_form(request, obj, **kwargs)
    #     # data request manager must update both en and ms fields
    #     for field in (
    #         "product_name_en",
    #         "product_description_en",
    #         "problem_statement_en",
    #         "solutions_developed_en",
    #     ):
    #         form.base_fields[field].required = False
    #     return form

    def save_model(self, request, obj: CommunityProduct, form, change) -> None:
        """
        Update date_approved timestamp and send out approval emails to product owner
        """
        if obj.status == "approved":
            obj.date_approved = timezone.now()
            with translation.override(obj.language):
                try:
                    mail.send(
                        sender=settings.DATA_GOV_MY_FROM_EMAIL,
                        recipients=obj.email,
                        language=obj.language,
                        template=self.COMMUNITY_PRODUCT_APPROVED_TEMPLATE,
                        context=dict(
                            name=obj.name,
                            product_name=getattr(obj, f"product_name_{obj.language}"),
                        ),
                        backend="datagovmy_ses",
                    )
                except Exception as e:
                    logging.error(e)
        elif obj.status == "rejected":
            obj.date_rejected = timezone.now()
            with translation.override(obj.language):
                try:
                    mail.send(
                        sender=settings.DATA_GOV_MY_FROM_EMAIL,
                        recipients=obj.email,
                        language=obj.language,
                        template=self.COMMUNITY_PRODUCT_REJECTED_TEMPLATE,
                        context=dict(
                            name=obj.name,
                            product_name=getattr(obj, f"product_name_{obj.language}"),
                            reason=getattr(obj, f"remark_{obj.language}"),
                        ),
                        backend="datagovmy_ses",
                    )
                except Exception as e:
                    logging.error(e)

        return super().save_model(request, obj, form, change)


admin.site.register(CommunityProduct, CommunityProductAdmin)
