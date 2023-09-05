from data_gov_my.explorers.General import GeneralTransportExplorer
from data_gov_my.models import PrasaranaTimeseries, PrasaranaTimeseriesCallout


class Prasarana(GeneralTransportExplorer):
    # General Data
    explorer_name = "Prasarana"

    # API handling

    # Data Populate (FIXME: update after data creates correct parquet files)
    data_populate = {
        "PrasaranaTimeseries": "https://storage.data.gov.my/dashboards/ktmb_timeseries.parquet",
        "PrasaranaTimeseriesCallout": "https://storage.data.gov.my/dashboards/ktmb_timeseries_callout.parquet",
    }

    TIMESERIES_MODEL = PrasaranaTimeseries
    TIMESERIES_CALLOUT_MODEL = PrasaranaTimeseriesCallout
