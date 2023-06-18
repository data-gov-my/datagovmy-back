from __future__ import annotations
from abc import ABC, abstractmethod
from django.utils.text import slugify
import numpy as np
import pandas as pd
from pydantic import BaseModel

from data_gov_my.utils.variable_structures import *
from data_gov_my.utils.variable_structures import GeneralChartVariables

# from variable_structures import *

"""
TODO:
1 add barmeter to existing charts - seem to have missed this earlier
2 add documentation to all the charts + validation format rationale, e.g. len(v) == 2: why
3 write down possible suggestions (if big change to format) for roshen to review: 
  changing existing structure <-- must refer to FE existing components data format!

4.Establish convention - rename_cols operation, variables afterwards will assume new column name, e.g. metrics table abbreviation (homepage_table_summary)
"""

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

        # rename cols
        df.rename(columns=variables.rename_cols, inplace=True)

        df = self.additional_preprocessing(variables, df)

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

    def additional_preprocessing(
        self, variables: GeneralChartVariables, df: pd.DataFrame
    ):
        return df


class BarChartBuilder(ChartBuilder):
    CHART_TYPE = "bar_chart"
    VARIABLE_MODEL = BarChartVariables

    def group_to_data(self, variables: BarChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.x].tolist()
        # FIXME: restrict y to list[str] only, instead must use rename_cols variable to rename?
        # maybe best to invert - list should maintain col name, axis values renamed. (y1..y4 needs to be manually defined.)
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
    VARIABLE_MODEL = HeatmapChartVariables

    # def additional_preprocessing(
    #     self, variables: HeatmapChartVariables, df: pd.DataFrame
    # ):
    #     df["id"] = df[variables.id]
    #     return df

    def group_to_data(self, variables: HeatmapChartVariables, group: pd.DataFrame):
        # TODO: in future, is 'id' column necessary?
        group.replace(variables.replace_vals, regex=True, inplace=True)
        data = group[variables.value_columns].transform(
            lambda x: [{"x": x.name, "y": y} for y in x]
        )
        data = data.to_numpy().flatten().tolist()
        return data


class TimeseriesBuilder(ChartBuilder):
    CHART_TYPE = "timeseries_chart"

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        df[column] = pd.to_datetime(df[column])
        df[column] = df[column].values.astype(np.int64) // 10**6
        return df

    def additional_preprocessing(
        self, variables: GeneralChartVariables, df: pd.DataFrame
    ):
        return super().additional_preprocessing(variables, df)


class TimeseriesSharedBuilder(ChartBuilder):
    CHART_TYPE = "timeseries_shared"
    # TODO: should this be combined w timeseriesbuilder, w shared=True/False option?


class LineBuilder(ChartBuilder):
    CHART_TYPE = "line_chart"
    VARIABLE_MODEL = LineChartVariables

    def group_to_data(self, variables: LineChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.x].tolist()
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


class CustomBuilder(ChartBuilder):  # DONE
    CHART_TYPE = "custom_chart"
    VARIABLE_MODEL = CustomChartVariables

    # FIXME: why not combine this w metrics table? seems quite similar

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

    def additional_preprocessing(
        self, variables: WaffleChartVariables, df: pd.DataFrame
    ):
        if len(variables.wanted) > 0:
            # FIXME: seems too specific for covid data, do we have to generalise it? (specify what column)
            df = df[df["age_group"].isin(variables.wanted)]
        return df

    def group_to_data(self, variables: WaffleChartVariables, group: pd.DataFrame):
        _key, _value = variables.dict_keys
        res = pd.Series(group[_value].values, index=group[_key]).to_dict()
        data = {}
        for k, v in variables.data_arr.items():
            if isinstance(v, str):
                data[k] = group[v].unique()[0]
            else:
                # FIXME: might be misunderstanding how this works, based on the singular waffle example
                col = next(iter(v))
                data[k] = group.loc[group[col] == v[col], k].iloc[0]
        res["data"] = [
            data
        ]  # FIXME: why is the structure like this? (need to wrap around list)
        return res


class HelpersCustomBuilder(ChartBuilder):
    CHART_TYPE = "helpers_custom"
    VARIABLE_MODEL = None

    def group_to_data(self):
        pass

    def build_chart(self, file_name: str) -> str:
        df = pd.read_parquet(file_name)
        df["state"].replace(STATE_ABBR, inplace=True)

        state_mapping = {}
        state_mapping["facility_types"] = df["type"].unique().tolist()
        state_mapping["state_district_mapping"] = {}

        for state in df["state"].unique():
            state_mapping["state_district_mapping"][state] = (
                df.groupby("state").get_group(state)["district"].unique().tolist()
            )

        return state_mapping


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

    # FIXME: consumer_price_index_choropleth fails bc district not slugified - should it be?

    def group_to_data(self, variables: ChoroplethChartVariables, group: pd.DataFrame):
        """
        FIXME: Exactly the same as line chart / bar chart - should it be the same then?
        """
        res = {}
        res["x"] = group[variables.x].tolist()
        res["y"] = {}
        for col in variables.y:
            res["y"][col] = group[col].tolist()
        return res


class JitterBuilder(ChartBuilder):
    CHART_TYPE = "jitter_chart"


class PyramidBuilder(ChartBuilder):
    CHART_TYPE = "pyramid_chart"
    VARIABLE_MODEL = PyramidChartVariables

    def group_to_data(self):
        pass

    def build_chart(self, file_name: str, variables: PyramidChartVariables) -> str:
        """
        TODO: this is taken directly from chart_builder function,
        there are no relevant test cases or existing active charts for reference - should make them and update accordingly.
        """
        col_range = variables["col_range"]
        suffix = variables["suffix"]
        keys = variables["keys"]

        df = pd.read_parquet(file_name)

        df[keys] = df[keys].apply(lambda x: x.lower().replace(" ", "_"))
        res = {}

        for k in df[keys].unique().tolist():
            res[k] = {}
            res[k]["x"] = list(col_range.keys())
            cur_df = df.groupby(keys).get_group(k)

            for s, v in suffix.items():
                s_values = [i + s for i in list(col_range.values())]
                res[k][v] = cur_df[s_values].values.tolist()[0]

        return res


class MetricsTableBuilder(ChartBuilder):
    CHART_TYPE = "metrics_table"
    VARIABLE_MODEL = MetricsTableVariables
    # FIXME: should combine with custom_chart with a variables.first = False ?

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


import pprint

if __name__ == "__main__":
    chart_type = "heatmap_chart"

    params = {
        "input": "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/blood_01_stock_snapshot.parquet",
        "variables": {
            "value_columns": ["a", "b", "ab", "o"],
            "keys": ["state"],
            "replace_vals": {"High": 0, "Safe": 1, "Low": 2, "Mid": 3},
        },
    }
    url = params["input"]
    variables = params["variables"]

    builder = ChartBuilder.create(chart_type)
    chart: pd.DataFrame = builder.build_chart(url, variables)
    pprint.pprint(chart)
