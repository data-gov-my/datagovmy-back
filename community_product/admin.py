from django.contrib import admin
from .models import CommunityProduct
from modeltranslation.admin import TranslationAdmin

# Register your models here.


class CommunityProductAdmin(TranslationAdmin):
    readonly_fields = [
        "name",
        "email",
        "institution",
        "created_at",
        "product_link",
        "product_year",
    ]
    list_filter = ["product_type"]
    MUST_TRANSLATE_FIELDS = [
        "product_name",
        "product_description",
        "problem_statement",
        "solutions_developed",
    ]

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


admin.site.register(CommunityProduct, CommunityProductAdmin)
