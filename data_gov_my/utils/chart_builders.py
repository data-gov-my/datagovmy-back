from __future__ import annotations
from abc import ABC, abstractmethod
from django.utils.text import slugify
import numpy as np
import pandas as pd
from pydantic import BaseModel

from data_gov_my.utils.variable_structures import *

# from variable_structures import *

STATE_ABBR = {
    "Johor": "jhr",
    "Kedah": "kdh",
    "Kelantan": "ktn",
    "Klang Valley": "kvy",
    "Melaka": "mlk",
    "Negeri Sembilan": "nsn",
    "Pahang": "phg",
    "Perak": "prk",
    "Perlis": "pls",
    "Pulau Pinang": "png",
    "Sabah": "sbh",
    "Sarawak": "swk",
    "Selangor": "sgr",
    "Terengganu": "trg",
    "W.P. Labuan": "lbn",
    "W.P. Putrajaya": "pjy",
    "W.P. Kuala Lumpur": "kul",
    "Malaysia": "mys",
}


class ChartBuilder(ABC):
    """
    General abstract class that contains common methods to build charts. This class should be extended to build cocnrete charts, e.g. timeseries.
    """

    subclasses = {}

    def __init_subclass__(cls, **kwargs) -> None:
        """
        Populate subclasses dictionary with CHART_TYPE as the key and the respective chart builder sub-classes as value.
        """
        super().__init_subclass__(**kwargs)
        cls.subclasses[cls.CHART_TYPE] = cls

    @classmethod
    def create(cls, chart_type: str) -> ChartBuilder:
        """
        Given chart_type, returns the corresponding chart builder class.
        """
        if chart_type not in cls.subclasses:
            raise ValueError(f"'{chart_type}' is not a valid chart type.")
        return cls.subclasses[chart_type]()

    @property
    @abstractmethod
    def CHART_TYPE(self) -> str:
        pass

    @property
    @abstractmethod
    def VARIABLE_MODEL(self) -> BaseModel:
        pass

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        df[column] = pd.to_datetime(df[column])
        df[column] = df[column].dt.strftime(format)
        return df

    def build_chart(self, file_name: str, variables: GeneralChartVariables) -> str:
        variables = self.VARIABLE_MODEL(**variables)
        df = pd.read_parquet(file_name)

        # pre-process column values (column names are considered reserve names)
        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if "district" in df.columns:  # District usually uses has spaces and Uppercase
            df["district"] = df["district"].apply(slugify)

        if "date" in df.columns:
            df = self.format_date(df)

        df = df.fillna(np.nan).replace({np.nan: variables.null_vals})

        if not variables.keys:
            return self.group_to_data(variables, df)

        result = {}
        value_cols = (
            variables.value_columns  # if value columns are defined, take as it is
            if variables.value_columns
            else list(
                set(df.columns) ^ set(variables.keys)
            )  # else, take all possible columns excluding key columns
        )
        df_groupby = df.groupby(variables.keys)
        for name, group in df_groupby:
            if isinstance(name, str):
                name = (name,)

            name = [str(n) for n in name]

            if len(variables.keys) > 1:
                current_level = result
                for i in range(len(name) - 1):
                    current_level = current_level.setdefault(name[i], {})
                current_level[name[-1]] = self.group_to_data(
                    variables, group
                )  ### children class must define how to handle each groups
            else:
                assert len(name) == 1
                result[name[0]] = self.group_to_data(variables, group)
        return result

    @abstractmethod
    def group_to_data(self, variables: GeneralChartVariables, group: pd.DataFrame):
        pass


class BarChartBuilder(ChartBuilder):
    CHART_TYPE = "bar_chart"
    VARIABLE_MODEL = BarChartVariables

    def group_to_data(self, variables: BarChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.x].tolist()
        # {renamed: actualcol}
        if isinstance(variables.y, list):
            if len(variables.y) == 1:
                res["y"] = group[variables.y[0]].tolist()
                return res
            variables.y = {f"y{i+1}": col for i, col in enumerate(variables.y)}

        for y_axis_label, col in variables.y.items():
            res[y_axis_label] = group[col].tolist()

        return res


class HeatMapBuilder(ChartBuilder):
    CHART_TYPE = "heatmap_chart"


class TimeseriesBuilder(ChartBuilder):
    CHART_TYPE = "timeseries_chart"


class LineBuilder(ChartBuilder):
    CHART_TYPE = "line_chart"


class BarmeterBuilder(ChartBuilder):
    CHART_TYPE = "bar_meter"


class CustomBuilder(ChartBuilder):  # DONE
    CHART_TYPE = "custom_chart"
    VARIABLE_MODEL = CustomChartVariables

    def group_to_data(self, variables: CustomChartVariables, group: pd.DataFrame):
        return group[variables.value_columns].to_dict("records")[0]


class SnapshotBuilder(ChartBuilder):
    CHART_TYPE = "snapshot_chart"


class WaffleBuilder(ChartBuilder):
    CHART_TYPE = "waffle_chart"


import pprint

if __name__ == "__main__":
    chart_type = "bar_chart"

    params = {
        "input": "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/covidepid_05_bar.parquet",
        "variables": {
            "keys": ["variable", "metric"],
            "x": "age",
            "y": {
                "unvax": "unvax",
                "partialvax": "partialvax",
                "fullyvax": "fullyvax",
                "boosted": "boosted",
            },
        },
    }
    url = params["input"]
    variables = params["variables"]

    builder = ChartBuilder.create(chart_type)
    chart: pd.DataFrame = builder.build_chart(url, variables)
    pprint.pprint(chart)
