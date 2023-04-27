from data_gov_my.utils.general_chart_helpers import *
from data_gov_my.utils.chart_builder import *
import os

"""
Segregates chart types,
into respective chart builders
"""


def build_chart(chart_type, data):
    variables = data["variables"]
    input_file = data["input"]

    if chart_type == "bar_chart":
        return bar_chart(input_file, variables)
    elif chart_type == "heatmap_chart":
        return heatmap_chart(input_file, variables)
    elif chart_type == "timeseries_chart":
        return timeseries_chart(input_file, variables)
    elif chart_type == "line_chart":
        return line_chart(input_file, variables)
    elif chart_type == "bar_meter":
        return bar_meter(input_file, variables)
    elif chart_type == "custom_chart":
        return custom_chart(input_file, variables)
    elif chart_type == "snapshot_chart":
        return snapshot_chart(input_file, variables)
    elif chart_type == "waffle_chart":
        return waffle_chart(input_file, variables)
    elif chart_type == "helpers_custom":
        return helpers_custom(input_file)
    elif chart_type == "map_lat_lon":
        return map_lat_lon(input_file, variables)
    elif chart_type == "choropleth_chart":
        return choropleth_chart(input_file, variables)
    elif chart_type == "jitter_chart":
        return jitter_chart(input_file, variables)
    elif chart_type == "pyramid_chart":
        return pyramid_chart(input_file, variables)
    elif chart_type == "metrics_table":
        return metrics_table(input_file, variables)
    elif chart_type == "timeseries_shared":
        return timeseries_shared(input_file, variables)
    elif chart_type == "query_values":
        return query_values(input_file, variables)
    else:
        return {}
