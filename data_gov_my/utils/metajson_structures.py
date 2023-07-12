from pydantic import BaseModel, validator, field_serializer, field_validator
from typing import Literal
from datetime import datetime
from data_gov_my.utils.chart_builders import ChartBuilder
from pydantic_core.core_schema import FieldValidationInfo
from typing import Optional


class DashboardChartModel(BaseModel):
    name: str
    chart_type: str
    chart_source: str
    data_as_of: datetime = datetime(2023, 7, 12, 11, 59)
    api_type: Literal["dynamic", "static", "individual_chart"]
    variables: dict  # will be validated individually for each chart
    api_params: list[str]

    @field_validator("api_params")
    def validate_api_params_against_keys(cls, v, info: FieldValidationInfo):
        keys = info.data["variables"].get("keys", [])

        if len(keys) < len(v) or keys[: len(v)] != v:
            raise ValueError(
                f"api_params {v} must be a subset of keys {keys} starting at index 0!"
            )

        return v

    @field_serializer("data_as_of")
    def serialize_date(self, data_as_of: datetime):
        return data_as_of.strftime("%Y-%m-%d %H:%M")

    @validator("chart_type")
    def valid_chart_type(cls, v: dict):
        if v not in ChartBuilder.subclasses.keys():
            raise ValueError(f"{v} is not a valid chart_type!")
        return v


class DashboardValidateModel(BaseModel):
    dashboard_name: str
    data_last_updated: datetime
    route: str
    manual_trigger: str
    required_params: list[str] = []
    optional_params: list[str] = []
    charts: dict[str, DashboardChartModel]

    @field_serializer("data_last_updated")
    def serialize_date(self, data_last_updated: datetime):
        return data_last_updated.strftime("%Y-%m-%d %H:%M")


class i18nValidateModel(BaseModel):
    route: str
    translation: dict


class _EmailTemplateValidateModel(BaseModel):
    name: str
    subject: str
    content: str
    html_content: str
    language: Literal["en-GB", "ms-MY"]


class FormValidateModel(BaseModel):
    send_email: bool
    validate_fields: list[str]
    email_template: list[_EmailTemplateValidateModel]


class ExplorerValidateModel(BaseModel):
    data_last_updated: datetime
    manual_trigger: str = "0"
    explorer_name: str
    route: str
    tables: dict[str, dict]

    @field_serializer("data_last_updated")
    def serialize_date(self, data_last_updated: datetime, _info):
        return data_last_updated.strftime("%Y-%m-%d %H:%M")


# class CatalogData(BaseModel):
#     catalog_filters: dict
#     metadata_neutral: dict
#     metadata_lang: dict[Literal["en", "bm"], dict]
#     chart: dict


# class DataCatalogVariable(BaseModel):
#     id: int
#     name: str
#     title_en: str
#     title_bm: str
#     desc_en: str
#     desc_bm: str
#     catalog_data: Optional[dict]


# TODO: data catalog metajson not that thoroughly handled,
# for proper refactor on this involves updating catalog_variable_classes as well (in future)
class _DataCatalogFileValidateModel(BaseModel):
    manual_trigger: str = "0"
    bucket: str
    file_name: str
    category: str
    category_en: str
    category_bm: str
    subcategory: str
    subcategory_en: str
    subcategory_bm: str
    description: dict[Literal["en", "bm"], str]
    link_parquet: Optional[str] = None
    link_csv: Optional[str] = None
    link_geojson: Optional[str] = None
    variables: list[dict]


class DataCatalogValidateModel(BaseModel):
    file: _DataCatalogFileValidateModel
