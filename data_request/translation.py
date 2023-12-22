from modeltranslation.translator import register, TranslationOptions
from data_request.models import Agency, DataRequest


@register(DataRequest)
class DataRequestTranslationOptions(TranslationOptions):
    fields = ("dataset_title", "dataset_description", "remark")
    fallback_languages = {"default": ("en", "ms")}


@register(Agency)
class AgencyTranslationOptions(TranslationOptions):
    fields = ("name",)
    fallback_languages = {"default": ("en", "ms")}
