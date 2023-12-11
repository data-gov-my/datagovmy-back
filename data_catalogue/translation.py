from modeltranslation.translator import register, TranslationOptions
from .models import (
    DataCatalogueMeta,
    Field,
    RelatedDataset,
    SiteCategory,
)


@register(Field)
class FieldTranslationOptions(TranslationOptions):
    fields = ("title", "description")


@register(RelatedDataset)
class RelatedDatasetTranslationOptions(TranslationOptions):
    fields = ("title", "description")


@register(SiteCategory)
class SiteCategoryTranslationOptions(TranslationOptions):
    fields = ("category", "subcategory")


@register(DataCatalogueMeta)
class DataCatalogueTranslationOptions(TranslationOptions):
    fields = ("title", "description", "methodology", "caveat", "publication")
