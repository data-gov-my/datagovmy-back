from data_gov_my.explorers.General import GeneralTransportExplorer
from data_gov_my.models import PrasaranaTimeseries, PrasaranaTimeseriesCallout


class Prasarana(GeneralTransportExplorer):
    # General Data
    explorer_name = "Prasarana"

    # API handling

    # Data Populate
    data_populate = {
        "PrasaranaTimeseries": "https://storage.data.gov.my/dashboards/prasarana_timeseries.parquet",
        "PrasaranaTimeseriesCallout": "https://storage.data.gov.my/dashboards/prasarana_timeseries_callout.parquet",
    }

    TIMESERIES_MODEL = PrasaranaTimeseries
    TIMESERIES_CALLOUT_MODEL = PrasaranaTimeseriesCallout
