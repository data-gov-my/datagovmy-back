REFRESH_VARIABLES = {
    "MetaJson": {
        "column_name": "dashboard_name",
        "directory": "/dashboards/",
    },
    "DashboardJson": {
        "column_name": "dashboard_name",
        "directory": "/dashboards/",
    },
    "CatalogJson": {
        "column_name": "file_src",
        "directory": "/catalog/",
    },
}

LANGUAGE_CHOICES = [("en-GB", "English"), ("ms-MY", "Bahasa Melayu")]

SITE_CHOICES = [
    ("datagovmy", "https://data.gov.my"),
    ("kkmnow", "https://data.moh.gov.my/"),
    ("opendosm", "https://open.dosm.gov.my/"),
]

CHART_TYPES = {
    "HBAR": {"parent": "Barv2", "constructor": "Bar"},
    "BAR": {"parent": "Barv2", "constructor": "Bar"},
    "STACKED_BAR": {"parent": "Barv2", "constructor": "Bar"},
    "LINE": {"parent": "Barv2", "constructor": "Bar"},
    "AREA": {"parent": "Timeseriesv2", "constructor": "Timeseries"},
    "TIMESERIES": {"parent": "Timeseriesv2", "constructor": "Timeseries"},
    "STACKED_AREA": {"parent": "Timeseriesv2", "constructor": "Timeseries"},
    "INTRADAY": {"parent": "Timeseriesv2", "constructor": "Timeseries"},
    "PYRAMID": {"parent": "Pyramidv2", "constructor": "Pyramid"},
    "HEATTABLE": {"parent": "Heattablev2", "constructor": "Heattable"},
    "CHOROPLETH": {"parent": "Choroplethv2", "constructor": "Choropleth"},
    "GEOCHOROPLETH": {"parent": "Choroplethv2", "constructor": "Choropleth"},
    "GEOPOINT": {"parent": "Geopointv2", "constructor": "Geopoint"},
    "GEOJSON": {"parent": "Geojsonv2", "constructor": "Geojson"},
    "SCATTER": {"parent": "Scatterv2", "constructor": "Scatter"},
    "TABLE": {"parent": "Tablev2", "constructor": "Table"},
}
