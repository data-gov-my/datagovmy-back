from __future__ import annotations
from abc import ABC, abstractmethod
from django.utils.text import slugify
import numpy as np
import pandas as pd
from pydantic import BaseModel, validator, ValidationError

from variable_structures import *

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

    def build_chart(self, file_name: str, variables) -> str:
        # validate variables in valid format (based on subclasses)
        variables = self.VARIABLE_MODEL(**variables)

        df = pd.read_parquet(file_name)

        # pre-process column values (column names are considered reserve names)
        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if "district" in df.columns:  # District usually uses has spaces and Uppercase
            df["district"] = df["district"].apply(slugify)

        if "date" in df.columns:
            self.format_date(df)

        df = df.fillna(variables.null_vals)

        return self.process_df_to_chart_data(df, variables)

    ## abstract method for common steps
    @abstractmethod
    def process_df_to_chart_data(self, df: pd.DataFrame, variables: BaseModel) -> dict:
        """
        Given dataframe, generate data in dict format to be returned as the chart data.
        """
        pass


class CustomBuilder(ChartBuilder):
    CHART_TYPE = "custom_chart"
    VARIABLE_MODEL = CustomChartVariables

    def process_df_to_chart_data(
        self, df: pd.DataFrame, variables: CustomChartVariables
    ) -> dict:
        keys = variables.keys
        columns = variables.columns
        res = {}
        grouped = df.groupby(keys)
        for name, group in grouped:
            current_lvl = res
            if isinstance(name, tuple):
                for n in name[:-1]:
                    current_lvl = current_lvl.setdefault(n, {})
                name = name[-1]
            current_lvl[name] = group[columns].to_dict("records")[0]

        return res

if __name__ == "__main__":
    v = {"keys": ["chart"], "columns": ["callout1", "callout2"], "null_vals": 0}
    url = "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/homepage_timeseries_callout.parquet"
    builder = ChartBuilder.create("custom_chart")
    chart = builder.build_chart(url, v)
