FRONTEND_ENDPOINTS = {
    "birthday_popularity": "/dashboard/birthday-explorer,/ms-MY/dashboard/birthday-explorer",
    "blood_donation": "/dashboard/blood-donation,/ms-MY/dashboard/blood-donation",
    "car_popularity": "/dashboard/car-popularity,/ms-MY/dashboard/car-popularity",
    "consumer_price_index": "/dashboard/consumer-prices,/ms-MY/dashboard/consumer-prices",
    "covid_epid": "/dashboard/covid-19,/ms-MY/dashboard/covid-19",
    "covid_vax": "/dashboard/covid-vaccination,/ms-MY/dashboard/covid-vaccination",
    "currency": "/dashboard/currency-in-circulation,/ms-MY/dashboard/currency-in-circulation",
    "dashboards": "/dashboard,/ms-MY/dashboard",
    "exchange_rates": "/dashboard/exchange-rates,/ms-MY/dashboard/exchange-rates",
    "gorss_domestic_product": "/dashboard/gdp,/ms-MY/dashboard/gdp",
    "homepage": "/,/ms-MY/",
    "interest_rates": "/dashboard/interest-rates,/ms-MY/dashboard/interest-rates",
    "international_reserves": "/dashboard/international-reserves,/ms-MY/dashboard/international-reserves",
    "money_measures": "/dashboard/money-supply,/ms-MY/dashboard/money-supply",
    "organ_donation": "/dashboard/organ-donation,/ms-MY/dashboard/organ-donation",
    "peka_b40": "/dashboard/peka-b40,/ms-MY/dashboard/peka-b40",
    "reserves": "/dashboard/reserve-money,/ms-MY/dashboard/reserve-money",
    "sekolahku": "/dashboard/sekolahku,/ms-MY/dashboard/sekolahku",
}

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
