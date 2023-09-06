from data_gov_my.explorers.General import GeneralTransportExplorer
from data_gov_my.models import KTMBTimeseries, KTMBTimeseriesCallout


class KTMB(GeneralTransportExplorer):
    # General Data
    explorer_name = "KTMB"

    # Data Populate
    data_populate = {
        "KTMBTimeseries": "https://storage.data.gov.my/dashboards/ktmb_timeseries.parquet",
        "KTMBTimeseriesCallout": "https://storage.data.gov.my/dashboards/ktmb_timeseries_callout.parquet",
    }

    TIMESERIES_MODEL = KTMBTimeseries
    TIMESERIES_CALLOUT_MODEL = KTMBTimeseriesCallout
