import calendar
from datetime import datetime, timedelta
from typing import List
from data_gov_my.explorers.General import General_Explorer
from rest_framework import exceptions
from django.http import JsonResponse
from django.apps import apps
from data_gov_my.models import DashboardJson, MetaJson


class BIRTHDAY_POPULARITY(General_Explorer):
    # General Data
    explorer_name = "BIRTHDAY_POPULARITY"

    # API handling
    required_params = ["explorer", "state"]

    def __init__(self):
        General_Explorer.__init__(self)

    def dates_in_year(self, year: int):
        """
        Given a year, return a list of dates (epoch milliseconds) within that year.
        A non-leap year returns 365 dates; leap year returns 366 dates in a single list.
        """
        start = datetime(year, 1, 1)
        end = datetime(year, 12, 31)
        epoch = datetime.utcfromtimestamp(0)
        dates = []
        while start <= end:
            epoch_ms = int((start - epoch).total_seconds() * 1000)
            dates.append(epoch_ms)
            start += timedelta(days=1)
        return dates

    def dates_in_month(self):
        """
        Return a list of dates (epoch milliseconds) for every
        first day of every month within a year starting from epoch millisecond 0.
        """
        start = datetime(1971, 1, 1)  # 1971 is an arbitrary year
        dates = []
        for month in range(1, 13):
            first_day_in_month = datetime(1971, month, 1)
            epoch_ms = int((first_day_in_month - start).total_seconds() * 1000)
            dates.append(epoch_ms)
        return dates

    def handle_api(self, request_params):
        """
        Handles the API requests, and returns the data accordingly.
        """
        if not self.is_params_exist(request_params):
            return JsonResponse({"status": 400, "message": "Bad Request"}, status=400)

        state = request_params["state"][0]

        # handle the data
        timeseries = DashboardJson.objects.get(
            dashboard_name="birthday_popularity", chart_name="timeseries"
        ).chart_data
        rank_table = DashboardJson.objects.get(
            dashboard_name="birthday_popularity", chart_name="rank_table"
        ).chart_data

        epochs = timeseries["data"][state]["x"]
        nationwide_births = timeseries["data"]["mys"]["births"]
        births = timeseries["data"][state]["births"]
        ranks = timeseries["data"][state]["rank"]

        # handle the query parameters (provide default if not given)
        start = (
            int(request_params["start"][0])
            if "start" in request_params
            else (datetime.utcfromtimestamp(0) + timedelta(milliseconds=epochs[0])).year
        )
        end = (
            int(request_params["end"][0])
            if "end" in request_params
            else (
                datetime.utcfromtimestamp(0) + timedelta(milliseconds=epochs[-1])
            ).year
        )
        groupByDay = (
            self.str2bool(request_params["groupByDay"][0])
            if "groupByDay" in request_params
            else True
        )
        birthday = (
            datetime.strptime(request_params["birthday"][0], "%Y-%m-%d")
            if "birthday" in request_params
            else None
        )

        hasLeap = any(calendar.isleap(y) for y in range(start, end + 1))
        start, end = datetime(year=start, month=1, day=1), datetime(
            year=end, month=12, day=31
        )
        count = [0] * (365 + hasLeap) if groupByDay else [0] * 12

        # aggregate births by day or month across years within start and end date range
        res = {}
        rank_table_res = {}
        for i, e in enumerate(epochs):
            date = datetime.utcfromtimestamp(0) + timedelta(milliseconds=e)
            if date == birthday:
                rank_table_res["rank"] = ranks[i]
                rank_table_res["state_total"] = births[i]
                rank_table_res["nationwide_total"] = nationwide_births[i]
            if start <= date <= end:
                pos = (
                    date.timetuple().tm_yday
                    + (
                        hasLeap
                        and not calendar.isleap(date.year)
                        and date.timetuple().tm_yday > 59
                    )
                    if groupByDay
                    else date.month
                )  # 59 = 1 March
                count[pos - 1] += births[i]

        if groupByDay:
            valid_dates = (
                self.dates_in_year(1972) if hasLeap else self.dates_in_year(1970)
            )
        else:
            valid_dates = self.dates_in_month()

        data_last_updated = MetaJson.objects.get(
            dashboard_name="birthday_popularity"
        ).dashboard_meta.get("data_last_updated", None)
        timeseries = {
            "data_as_of": timeseries.get("data_as_of", None),
            "x": valid_dates,
            "y": count,
        }
        res["data_last_updated"] = data_last_updated
        res["timeseries"] = timeseries
        if birthday is not None:
            rank_table_res["data_as_of"] = rank_table.get("data_as_of", None)
            rank_table_res["popularity"] = rank_table["data"][state][str(birthday.year)]
            res["rank_table"] = rank_table_res
        return JsonResponse(res, status=200)
