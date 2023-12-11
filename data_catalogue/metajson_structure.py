from pydantic import BaseModel, HttpUrl


class SiteCategory(BaseModel):
    site: str
    category_en: str
    category_ms: str
    subcategory_en: str
    subcategory_ms: str


class Field(BaseModel):
    name: str
    title_en: str
    title_ms: str
    description_en: str
    description_ms: str


class RelatedDataset(BaseModel):
    id: str
    # TODO: related datasets should only have the ids? title and description refers to other?
    title_en: str
    title_ms: str
    description_en: str
    description_ms: str


# class Table(BaseModel):
#     precision: int
#     filter_columns: list[str] = []
#     freeze_columns: list[str] = []


class DataCatalogueValidateModel(BaseModel):
    bucket: str
    filename: str
    exclude_openapi: bool
    manual_trigger: str
    data_as_of: str
    last_updated: str
    next_update: str
    methodology_en: str
    methodology_ms: str
    caveat_en: str
    caveat_ms: str
    publication_en: str
    publication_ms: str
    site_category: list[SiteCategory]
    description_en: str
    description_ms: str
    link_parquet: HttpUrl
    link_csv: HttpUrl
    link_preview: HttpUrl
    frequency: str
    geography: list[str]
    demography: list[str]
    dataset_begin: str
    dataset_end: str
    data_source: list[str]
    fields: list[Field] = []
    # table: Table
    dataviz: list[dict] = []  # TODO: table @ dataviz[0] enforcd
    translations: dict = {}
    related_datasets: list[RelatedDataset] = []
