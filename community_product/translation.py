from modeltranslation.translator import register, TranslationOptions
from .models import CommunityProduct


@register(CommunityProduct)
class CommunityProductTranslationOptions(TranslationOptions):
    fields = (
        "product_name",
        "product_description",
        "problem_statement",
        "solutions_developed",
    )
    fallback_languages = {"default": ("en", "ms")}
