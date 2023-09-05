from __future__ import annotations

from abc import ABC, abstractmethod

import numpy as np
import pandas as pd
from django.utils.text import slugify
from pydantic import BaseModel

from data_gov_my.utils.variable_structures import *

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

    VARIABLE_MODEL = GeneralChartVariables
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
        """
        Refers to what kind of chart builder the class is, e.g. "timeseries_chart", "bar_meter" etc.
        """
        pass

    @property
    @abstractmethod
    def VARIABLE_MODEL(self) -> BaseModel:
        """
        The variable class used to type-check and validate each chart variables' convention.
        """
        pass

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        """
        Formats the date column and returns the whole dataframe. The default column name is "date".
        """
        df[column] = pd.to_datetime(df[column])
        df[column] = df[column].dt.strftime(format)
        return df

    def pre_process(self, df: pd.DataFrame, variables: GeneralChartVariables):
        """
        - Pre-process the column values, reserve column names include 'state', 'district', 'date'.
        - Fills the null values as defined in variables, else defaults to None
        - Renames the columns based on variables definition.
        - Calls `additional_preprocessing()`, which is usually overriden in children builder classes.
        """
        # pre-process column values (column names are considered reserve names)
        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if (
            "district" in df.columns and "district" in variables.keys
        ):  # District usually uses has spaces and Uppercase
            df["district"] = df["district"].apply(slugify)

        if "date" in df.columns:
            df = self.format_date(df)

        df = df.fillna(np.nan).replace({np.nan: variables.null_vals})

        # rename cols & vals
        df.rename(columns=variables.rename_cols, inplace=True)
        df.replace(to_replace=variables.replace_vals, inplace=True)
        for col, wanted in variables.filter.items():
            df = df[df[col].isin(wanted)]
        df = self.additional_preprocessing(variables, df)

        return df

    def additional_preprocessing(
        self, variables: GeneralChartVariables, df: pd.DataFrame
    ):
        """
        Overriden in children builder classes to pre-process the dataframe before producing the output results.
        """
        return df

    def additional_postprocessing(
        self, variables: GeneralChartVariables, df: pd.DataFrame, result: dict
    ):
        """
        Overriden in children builder classes to post-process the results.
        """
        return result

    def build_chart(self, file_name: str, variables: GeneralChartVariables) -> str:
        """
        General chart building procedure based on common variable keys.
        1. Validate variables and pre-process dataframe.
        2. Get groupby sub-dataframes if there are defined `keys` in variables, else pass the full dataframe to `group_to_data()`.
        3. Children class will have to define abstract method `group_to_data()`, which will process how each groups are formatted for the final result output.
        4. Result is passed through additional post-processing and will be transformed where necessary, as defined in children builer classes.
        """
        variables = self.VARIABLE_MODEL(**variables)
        df = pd.read_parquet(file_name)

        df = self.pre_process(df, variables)

        if not variables.keys:
            result = self.group_to_data(variables, df)

        else:
            result = {}
            # value_cols = (
            #     variables.value_columns  # if value columns are defined, take as it is
            #     if variables.value_columns
            #     else list(
            #         set(df.columns) ^ set(variables.keys)
            #     )  # else, take all possible columns excluding key columns
            # )
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

        result = self.additional_postprocessing(variables, df, result)

        return result

    @abstractmethod
    def group_to_data(
        self, variables: GeneralChartVariables, group: pd.DataFrame
    ) -> dict:
        """
        Sub-dataframe broken down from variables.keys is individually processed into final dict result.
        """
        pass


class BarChartBuilder(ChartBuilder):
    CHART_TYPE = "bar_chart"
    VARIABLE_MODEL = BarChartVariables

    def group_to_data(self, variables: BarChartVariables, group: pd.DataFrame) -> dict:
        """
        Converts grouped data into a format suitable for generating a bar chart.
        - The resulting dictionary has the following structure:
            - "x": A list of x-axis values obtained from the `variables.x` column of the `group` DataFrame.
            - "y": A list of y-axis values obtained from the `variables.y` column of the `group` DataFrame.
        """
        res = {}
        res[variables.x] = group[variables.x].tolist()
        for col in variables.y:
            res[col] = group[col].tolist()
        return res


class HeatMapBuilder(ChartBuilder):
    CHART_TYPE = "heatmap_chart"
    VARIABLE_MODEL = HeatmapChartVariables

    def group_to_data(self, variables: HeatmapChartVariables, group: pd.DataFrame):
        group.rename(
            columns={variables.x: "x", variables.y: "y", variables.z: "z"}, inplace=True
        )
        return group[["x", "y", "z"]].to_dict("records")


class TimeseriesBuilder(ChartBuilder):
    CHART_TYPE = "timeseries_chart"
    VARIABLE_MODEL = TimeseriesChartVariables

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        """ """
        df[column] = pd.to_datetime(df[column])

        df[column] = df[column].values.astype(np.int64) // 10**6
        return df

    def additional_postprocessing(
        self, variables: TimeseriesChartVariables, df: pd.DataFrame, result: dict
    ):
        for col in variables.constants:
            result[col] = df[col].unique().tolist()
        return result

    def group_to_data(self, variables: TimeseriesChartVariables, group: pd.DataFrame):
        res = {}
        for col in variables.value_columns:
            res[col] = group[col].tolist()
        return res


class LineBuilder(ChartBuilder):
    CHART_TYPE = "line_chart"
    VARIABLE_MODEL = LineChartVariables

    def group_to_data(self, variables: LineChartVariables, group: pd.DataFrame):
        res = {}
        res[variables.x] = group[variables.x].tolist()
        for col in variables.y:
            res[col] = group[col].tolist()
        return res


class BarmeterBuilder(ChartBuilder):
    CHART_TYPE = "bar_meter"
    VARIABLE_MODEL = BarMeterVariables

    def group_to_data(self, variables: BarMeterVariables, group: pd.DataFrame):
        result = {} if variables.sub_keys else []
        for d in variables.axis_values:
            for key, value in d.items():
                x_values = group[key].values
                y_values = group[value].values
                if variables.sub_keys:
                    result[value] = [
                        {"x": x, "y": y} for x, y in zip(x_values, y_values)
                    ]
                else:
                    result.extend(
                        [{"x": x, "y": y} for x, y in zip(x_values, y_values)]
                    )
        return result


class CustomBuilder(ChartBuilder):
    CHART_TYPE = "custom_chart"
    VARIABLE_MODEL = CustomChartVariables

    def group_to_data(self, variables: CustomChartVariables, group: pd.DataFrame):
        return group[variables.value_columns].to_dict("records")[0]


class SnapshotBuilder(ChartBuilder):
    CHART_TYPE = "snapshot_chart"
    VARIABLE_MODEL = SnapshotChartVariables

    def group_to_data(self, variables: SnapshotChartVariables, group: pd.DataFrame):
        record_list = list(variables.data.keys())
        record_list.append("index")
        record_list.append(variables.main_key)
        group["index"] = range(len(group[variables.main_key]))
        changed_cols = {}
        for k, v in variables.data.items():
            if variables.replace_word != "":
                changed_cols = {x: x.replace(k, variables.replace_word) for x in v}
            for i in v:
                if group[i].dtype == "object":
                    group[i] = group[i].astype(str)

            group[k] = (
                group[v]
                .rename(columns=changed_cols)
                .apply(lambda s: s.to_dict(), axis=1)
            )

        res_dict = group[record_list].to_dict(orient="records")
        return res_dict


class WaffleBuilder(ChartBuilder):
    CHART_TYPE = "waffle_chart"
    VARIABLE_MODEL = WaffleChartVariables

    def group_to_data(self, variables: WaffleChartVariables, group: pd.DataFrame):
        _key, _value = variables.dict_keys
        res = pd.Series(group[_value].values, index=group[_key]).to_dict()
        data = {}
        for k, v in variables.data_arr.items():
            if isinstance(v, str):
                data[k] = group[v].unique()[0]
            else:
                col = next(iter(v))
                data[k] = group.loc[group[col] == v[col], k].iloc[0]
        res["data"] = [data]
        return res


class MapLatLonBuilder(ChartBuilder):
    CHART_TYPE = "map_lat_lon"
    VARIABLE_MODEL = GeneralChartVariables

    def group_to_data(self, variables: GeneralChartVariables, group: pd.DataFrame):
        if variables.value_columns:
            return group[variables.value_columns].to_dict("records")
        else:
            return []


class ChoroplethBuilder(ChartBuilder):
    CHART_TYPE = "choropleth_chart"
    VARIABLE_MODEL = ChoroplethChartVariables

    def group_to_data(self, variables: ChoroplethChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.x].tolist()
        res["y"] = {}
        for col in variables.y:
            res["y"][col] = group[col].tolist()
        return res


class JitterBuilder(ChartBuilder):
    CHART_TYPE = "jitter_chart"
    VARIABLE_MODEL = JitterChartVariables

    def group_to_data(self, variables: GeneralChartVariables, group: pd.DataFrame):
        pass

    def build_chart(self, file_name: str, variables: JitterChartVariables) -> str:
        df: pd.DataFrame = pd.read_parquet(file_name)
        variables = self.VARIABLE_MODEL(**variables)
        df = df.fillna(np.nan).replace({np.nan: variables.null_vals})
        res = {}

        df[variables.keys] = df[variables.keys].apply(
            lambda x: x.lower().replace(" ", "_")
        )
        key_vals = (
            df[variables.keys].unique().tolist()
        )  # Handles just 1 key ( as of now )

        for k in key_vals:
            res[k] = {}

            for key, val in variables.columns.items():
                res[k][key] = []

                for col in val:
                    x_val = col + "_x"
                    y_val = col + "_y"

                    cols_rename = {x_val: "x", y_val: "y", id: "id"}
                    cols_inv = ["area", x_val, y_val]

                    if variables.tooltip:
                        t_val = col + "_t"
                        cols_rename[t_val] = "tooltip"
                        cols_inv.append(t_val)

                    temp_df = df.groupby(variables.keys).get_group(k)[cols_inv]
                    temp_df = temp_df.rename(columns=cols_rename)
                    data = temp_df.to_dict("records")
                    res[k][key].append({"key": col, "data": data})

        return res


class PyramidBuilder(ChartBuilder):
    CHART_TYPE = "pyramid_chart"
    VARIABLE_MODEL = PyramidChartVariables

    def group_to_data(self, variables: PyramidChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.label_column].to_list()
        res[variables.y1] = group[variables.y1].to_list()
        res[variables.y2] = group[variables.y2].to_list()
        return res


class MetricsTableBuilder(ChartBuilder):
    CHART_TYPE = "metrics_table"
    VARIABLE_MODEL = MetricsTableVariables

    def group_to_data(self, variables: MetricsTableVariables, group: pd.DataFrame):
        if variables.value_columns:
            return group[variables.value_columns].to_dict("records")
        else:
            return []


class QueryValuesBuilder(ChartBuilder):
    CHART_TYPE = "query_values"
    VARIABLE_MODEL = QueryValuesVariables

    def group_to_data(self):
        pass

    def build_chart(self, file_name: str, variables: QueryValuesVariables) -> str:
        df = pd.read_parquet(file_name)
        variables: QueryValuesVariables = self.VARIABLE_MODEL(**variables)

        if variables.sort_values:
            sort_cols, ascending = (
                variables.sort_values.by,
                variables.sort_values.ascending,
            )
            df = df.sort_values(by=sort_cols, ascending=ascending)

        if len(variables.columns) == 1:
            return list(df[variables.columns[0]].unique())

        if variables.flat:
            keys = df.drop_duplicates(subset=variables.columns)[variables.columns]
            return keys.to_dict(orient="records")

        res = {}
        for keys, v in df.groupby(variables.columns[:-1])[variables.columns[-1]]:
            d = res
            val = v.unique().tolist()
            keys = [keys] if isinstance(keys, str) else keys
            for k in keys:
                if k not in d:
                    d[k] = {}
                parent = d
                d = d[k]
            parent[k] = val

        return res
