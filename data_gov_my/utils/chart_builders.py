from __future__ import annotations
from abc import ABC, abstractmethod
from django.utils.text import slugify
import numpy as np
import pandas as pd
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

        if "district" in df.columns:  # District usually uses has spaces and Uppercase
            df["district"] = df["district"].apply(slugify)

        if "date" in df.columns:
            df = self.format_date(df)

        df = df.fillna(np.nan).replace({np.nan: variables.null_vals})

        # rename cols & vals
        df.rename(columns=variables.rename_cols, inplace=True)
        df.replace(to_replace=variables.replace_vals, inplace=True)
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
    """
    FIXME:
    - Suggest restricting BarCharVariables `y` to list[str] only, instead must use rename_cols variable to rename?
    - In general, perhaps best to standardise `rename_cols` being called first, then remaining variables can directly reference the
      new column name, instead of having to use a dict. This applies to other charts as well (e.g. timeseries)
      e.g.:

      ## OLD
    "variables": {
            "keys": [
                "variable",
                "metric"
            ],
            "x": "age",
            "y": {
                "unvaccinated": "unvax",
                "partialvax": "partialvax",
                "fullyvax": "fullyvax",
                "boosted": "boosted"
            }
        }

      ## PROPOSED NEW
    "variables": {
            "keys": [
                "variable",
                "metric"
            ],
            "rename_cols": {"unvax": "unvaccinated"}
            "x": "age",
            "y": ["unvaccinated", "partialvax", "fullyvax", "boosted]
        }
    """

    CHART_TYPE = "bar_chart"
    VARIABLE_MODEL = BarChartVariables

    def group_to_data(self, variables: BarChartVariables, group: pd.DataFrame) -> dict:
        """
        Converts grouped data into a format suitable for generating a bar chart.
        - The resulting dictionary has the following structure:
            - "x": A list of x-axis values obtained from the `variables.x` column of the `group` DataFrame.
            - "y" or "y1", "y2", ...: Depending on the structure of `variables.y`, the corresponding y-axis values are included in the dictionary.
            If 'variables.y' is a list containing a single string, the resulting dictionary will have a single "y" key for the y-axis values.
            If `variables.y` is a list containing multiple strings, the y-axis values are obtained from that column and stored under the key "y{i}".
            If 'variables.y' is a dictionary mapping labels to column names, each column's values are stored under the respective label in the dictionary.

        """
        res = {}
        res["x"] = group[variables.x].tolist()
        if isinstance(variables.y, list):
            if len(variables.y) == 1:
                res["y"] = group[variables.y[0]].tolist()
                return res
            variables.y = {f"y{i+1}": col for i, col in enumerate(variables.y)}

        for y_axis_label, col in variables.y.items():
            res[y_axis_label] = group[col].tolist()

        return res


class HeatMapBuilder(ChartBuilder):
    """
    TODO:
    - `id` column is removed:
    If its alright i'd like to remove the ID column (based on existing implementation), or atleast enforce id should only be the last value in keys column for correctness,
    since it feels logically inacurrate if the id column != last value in keys column, e.g. refer to test case test_heatmap_chart_multi_cols() expected results,
    the id is California, but data includes all states.

    Referring to FE heatmap component, doesn't seem like there's an ID required.

    Since there are no existing active charts using it, perhaps should KIV till its needed and adjust accordingly then.
    """

    CHART_TYPE = "heatmap_chart"
    VARIABLE_MODEL = HeatmapChartVariables

    # def additional_preprocessing(
    #     self, variables: HeatmapChartVariables, df: pd.DataFrame
    # ):
    #     df["id"] = df[variables.id]
    #     return df

    def group_to_data(self, variables: HeatmapChartVariables, group: pd.DataFrame):
        group.replace(variables.replace_vals, regex=True, inplace=True)
        data = group[variables.value_columns].transform(
            lambda x: [{"x": x.name, "y": y} for y in x]
        )
        data = data.to_numpy().flatten().tolist()
        return data


class TimeseriesBuilder(ChartBuilder):
    """
    FIXME: DATA_RANGE in variables not handled as of now, don't see an existing chart for it (?)
    """

    CHART_TYPE = "timeseries_chart"
    VARIABLE_MODEL = TimeseriesChartVariables

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        """ """
        df[column] = pd.to_datetime(df[column])

        df[column] = df[column].values.astype(np.int64) // 10**6
        return df

    def group_to_data(self, variables: TimeseriesChartVariables, group: pd.DataFrame):
        """
        FIXME: `values` should perhaps be removed and standardised to `value_columns` and `rename_cols`..?
        """
        res = {}
        for key, col in variables.values.items():
            res[key] = group[col].tolist()
        return res


class TimeseriesSharedBuilder(ChartBuilder):
    """
    FIXME: DATA_RANGE in variables not handled as of now, don't see an existing chart for it (?)
    TODO: should this be combined w timeseriesbuilder, w shared=True/False option?
    """

    CHART_TYPE = "timeseries_shared"
    VARIABLE_MODEL = TimeseriesSharedVariables

    def format_date(self, df: pd.DataFrame, column="date", format="%Y-%m-%d"):
        df[column] = pd.to_datetime(df[column])

        df[column] = df[column].values.astype(np.int64) // 10**6
        return df

    def additional_postprocessing(
        self, variables: TimeseriesSharedVariables, df: pd.DataFrame, result: dict
    ):
        for key, col in variables.constant.items():
            result[key] = df[col].unique().tolist()
        return result

    def group_to_data(self, variables: TimeseriesSharedVariables, group: pd.DataFrame):
        """
        FIXME: values should perhaps be removed and standardised to value_columns and rename_cols..?
        """
        res = {}
        for key, col in variables.attributes.items():
            res[key] = group[col].tolist()
        return res


class LineBuilder(ChartBuilder):
    """
    FIXME: Currently, variables.y only supports list[str], instead of dict[str,str] like bar_chart, if decided on whether to decouple renaming and choosing y columns,
    this can remain as it is, else should update accoridngly.
    """

    CHART_TYPE = "line_chart"
    VARIABLE_MODEL = LineChartVariables

    def group_to_data(self, variables: LineChartVariables, group: pd.DataFrame):
        res = {}
        res["x"] = group[variables.x].tolist()
        for col in variables.y:
            res[col] = group[col].tolist()
        return res


class BarmeterBuilder(ChartBuilder):
    """
    FIXME: for axis values, why not just dict[str,str], instead of list[dict[str, str]]?
    """

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
    """
    TODO:
    - Should we combine this with metrics_table, and add a `only_first` option for custom chart? The implementation is very similar, but custom builder
    returns the first item in the list, instead of the full list like metrics table.
    """

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
    """
    TODO: this is taken directly from chart_builder function,
    there are no relevant test cases or existing active charts for reference - should make them and update accordingly.
    """

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
    """
    TODO:
    This one may need more concrete examples for a proper implementation? Perhaps this should also be absored into custom charts (with metrics table)
    """

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
        FIXME: The logic seems to be the same as line chart / bar chart - should they be the combined?
        """
        res = {}
        res["x"] = group[variables.x].tolist()
        res["y"] = {}
        for col in variables.y:
            res["y"][col] = group[col].tolist()
        return res


class JitterBuilder(ChartBuilder):
    """
    TODO: this is taken directly from chart_builder function,
    there are no relevant test cases or existing active charts for reference - should make them and update accordingly.
    """

    CHART_TYPE = "jitter_chart"
    VARIABLE_MODEL = JitterChartVariables

    def group_to_data(self, variables: GeneralChartVariables, group: pd.DataFrame):
        pass

    def build_chart(self, file_name: str, variables: JitterChartVariables) -> str:
        df: pd.DataFrame = pd.read_parquet(file_name)
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
    """
    TODO: this is taken directly from chart_builder function,
    there are no relevant test cases or existing active charts for reference - should make them and update accordingly.
    """

    CHART_TYPE = "pyramid_chart"
    VARIABLE_MODEL = PyramidChartVariables

    def group_to_data(self):
        pass

    def build_chart(self, file_name: str, variables: PyramidChartVariables) -> str:
        col_range = variables.col_range
        suffix = variables.suffix
        keys = variables.keys

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
    """
    # FIXME: should combine with custom_chart with a variables.first = False ?
    """

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
