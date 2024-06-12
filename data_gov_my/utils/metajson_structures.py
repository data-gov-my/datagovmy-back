from __future__ import annotations
from datetime import date, datetime
import re
from typing import Literal, Optional

from pydantic import (
    BaseModel,
    field_serializer,
    field_validator,
    model_validator,
    validator,
)

from data_gov_my.utils.chart_builders import ChartBuilder

SITES = Literal["datagovmy", "kkmnow", "opendosm", "databnm"]


class DashboardChartModel(BaseModel):
    chart_type: str
    chart_source: str
    data_as_of: str
    api_type: Literal["dynamic", "static", "individual_chart"]
    variables: dict  # will be validated individually for each chart
    api_params: list[str]

    @validator("chart_type")
    def valid_chart_type(cls, v: dict):
        if v not in ChartBuilder.subclasses.keys():
            raise ValueError(f"{v} is not a valid chart_type!")
        return v

    @field_validator("data_as_of")
    def proper_date_format(cls, v):
        try:
            # Attempt to parse the input string as a datetime with format YYYY-MM-DD HH:MM
            datetime.strptime(v, "%Y-%m-%d %H:%M")
            return v
        except ValueError:
            pass

        try:
            # Attempt to parse the input string as a datetime with format YYYY-MM-DD
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            pass

        try:
            # Attempt to parse the input string as a datetime with format YYYY-MM
            datetime.strptime(v, "%Y-%m")
            return v
        except ValueError:
            pass

        # Attempt to parse the input string as quarterly and yearly data
        if re.match(r"^\d{4}-Q[1-4]$", v) or re.match(r"^\d{4}$", v):
            return v

        raise ValueError(
            "Invalid data_as_of formats! (should be %Y-%m-%d %H:%M, %Y-%m, %Y or r'^\d{4}-Q[1-4]$')"
        )


class DashboardValidateModel(BaseModel):
    data_last_updated: datetime
    data_next_update: Optional[datetime] = None
    route: str
    sites: list[SITES]
    manual_trigger: str
    required_params: list[str] = []
    optional_params: list[str] = []
    charts: dict[str, DashboardChartModel]

    @field_serializer("data_last_updated")
    def serialize_date_last_updated(self, data_last_updated: datetime):
        return data_last_updated.strftime("%Y-%m-%d %H:%M")

    @field_serializer("data_next_update")
    def serialize_date_next_update(self, data_next_update: datetime):
        return data_next_update.strftime("%Y-%m-%d %H:%M") if data_next_update else None


class i18nValidateModel(BaseModel):
    route: str | None
    sites: list[SITES]
    translation: dict


class _EmailTemplateValidateModel(BaseModel):
    name: str
    subject: str
    content: str
    html_content: str
    language: Literal["en-GB", "ms-MY", "en", "ms"]


class FormValidateModel(BaseModel):
    send_email: bool
    validate_fields: list[str]
    email_template: list[_EmailTemplateValidateModel]


class ExplorerValidateModel(BaseModel):
    data_last_updated: datetime
    data_next_update: Optional[datetime] = None
    manual_trigger: str = "0"
    explorer_name: str
    route: str
    sites: list[SITES]
    tables: dict[str, dict]

    @field_serializer("data_last_updated")
    def serialize_date_last_updated(self, data_last_updated: datetime):
        return data_last_updated.strftime("%Y-%m-%d %H:%M")

    @field_serializer("data_next_update")
    def serialize_date_next_update(self, data_next_update: datetime):
        return data_next_update.strftime("%Y-%m-%d %H:%M") if data_next_update else None


class _PublicationResourceValidateModel(BaseModel):
    resource_id: int
    resource_type: str
    resource_name: str
    resource_link: str


class _PublicationLangValidateModel(BaseModel):
    title: str
    description: str
    publication_type_title: str
    resources: list[_PublicationResourceValidateModel]


class PublicationValidateModel(BaseModel):
    publication: str
    publication_type: str
    release_date: date
    frequency: str
    geography: list
    demography: list
    en: _PublicationLangValidateModel
    bm: _PublicationLangValidateModel

    @model_validator(mode="after")
    def validate_api_params_against_keys(cls, v: PublicationValidateModel):
        resource_en = v.en.resources
        resource_bm = v.bm.resources

        if len(resource_bm) != len(resource_en):
            raise ValueError(f"Resources of different language must be same length!")

        return v


class PublicationDocumentationValidateModel(BaseModel):
    publication: str
    documentation_type: str
    publication_type: str
    release_date: date
    en: _PublicationLangValidateModel
    bm: _PublicationLangValidateModel

    @model_validator(mode="after")
    def validate_api_params_against_keys(cls, v: PublicationValidateModel):
        resource_en = v.en.resources
        resource_bm = v.bm.resources

        if len(resource_bm) != len(resource_en):
            raise ValueError(f"Resources of different language must be same length!")

        return v


class _PublicationUpcomingLangModel(BaseModel):
    title: str
    publication_type_title: str
    product_type: str
    release_series: str


class PublicationUpcomingValidateModel(BaseModel):
    manual_trigger: str | int | bool
    parquet_link: str
