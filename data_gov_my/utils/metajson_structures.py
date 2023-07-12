from enum import Enum
from pydantic import BaseModel, validator, field_serializer
from typing import Literal
from datetime import datetime
from data_gov_my.utils.chart_builders import ChartBuilder
from data_gov_my.utils.variable_structures import GeneralChartVariables


class DashboardChartModel(BaseModel):
    name: str
    chart_type: str
    chart_source: str
    data_as_of: datetime
    api_type: Literal["dynamic", "static", "individual_chart"]
    variables: dict

    @field_serializer("data_as_of")
    def serialize_date(self, data_as_of: datetime, _info):
        return data_as_of.strftime("%Y-%m-%d %H:%M")

    @validator("chart_type")
    def valid_chart_type(cls, v: dict):
        if v not in ChartBuilder.subclasses.keys():
            raise ValueError(f"{v} is not a valid chart_type!")
        return v

    @validator("variables")
    def valid_variables_by_chart_type(cls, v: dict, values: dict, **kwargs):
        # TODO: get the correct variables, then validate (maybe can remove from chart builder then)
        if "chart_type" not in values:
            raise ValueError(
                "chart_type must be defined for the chart in Dashboard metajsons!"
            )
        chart_builder: ChartBuilder = ChartBuilder.subclasses[values["chart_type"]]
        variable_model = chart_builder.VARIABLE_MODEL
        validated_variable = variable_model(**v)
        return validated_variable.model_dump_json()


class DashboardModel(BaseModel):
    dashboard_name: str
    data_last_updated: datetime
    route: str
    manual_trigger: str
    required_params: list[str] = []
    optional_params: list[str] = []
    charts: dict[str, DashboardChartModel]

    @field_serializer("data_last_updated")
    def serialize_date(self, data_last_updated: datetime, _info):
        return data_last_updated.strftime("%Y-%m-%d %H:%M")
