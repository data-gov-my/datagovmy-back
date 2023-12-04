from modeltranslation.translator import register, TranslationOptions
from data_request.models import DataRequest


@register(DataRequest)
class DataRequestTranslationOptions(TranslationOptions):
    fields = ("dataset_title", "dataset_description")
    fallback_languages = {"default": ("en", "ms")}
