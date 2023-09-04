from datetime import date, datetime
from itertools import groupby

from django.http import JsonResponse
from rest_framework import response

from data_gov_my.explorers.General import General_Explorer
from data_gov_my.models import ExplorersUpdate, KTMBTimeseries, KTMBTimeseriesCallout

EPOCH = datetime(1970, 1, 1)


def unix_time_millis(dt):
    if isinstance(dt, date):
        dt = datetime.combine(dt, datetime.min.time())
    return int((dt - EPOCH).total_seconds() * 1000.0)


class KTMB(General_Explorer):
    # General Data
    explorer_name = "KTMB"

    # API handling
    required_params = ["service", "origin", "destination"]

    # Data Populate
    data_populate = {
        "KTMBTimeseries": "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/ktmb_timeseries.parquet",
        "KTMBTimeseriesCallout": "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/ktmb_timeseries_callout.parquet",
    }

    def handle_api(self, request_params):
        """
        Handles the API requests, and returns the data accordingly.
        """
        if str(request_params.get("dropdown", [False])[0]).lower() == "true":
            dropdown = self.get_dropdown()
            return response.Response(dropdown)

        # compile chart data if dropdown param not true
        if not self.is_params_exist(request_params):
            return JsonResponse(
                {
                    "status": 400,
                    "message": f"Please provide the following params: {self.required_params}",
                },
                status=400,
            )

        service = request_params.get("service", [None])[0]
        origin = request_params.get("origin", [None])[0]
        destination = request_params.get("destination", [None])[0]

        timeseries = self.get_timeseries(service, origin, destination)
        callout = self.get_timeseries_callout(service, origin, destination)
        res = dict(timeseries=timeseries, timeseries_callout=callout)
        return response.Response(res)

    def get_dropdown(self):
        queryset = (
            KTMBTimeseriesCallout.objects.order_by("service", "origin")
            .values("service", "origin", "destination")
            .distinct()
        )
        d = {}
        for service, group in groupby(queryset, lambda x: x["service"]):
            d[service] = {}
            for origin, inner_group in groupby(group, lambda x: x["origin"]):
                d[service][origin] = [data["destination"] for data in inner_group]
        return d

    def get_timeseries(self, service, origin, destination):
        # timeseries = (
        #     KTMBTimeseries.objects.filter(
        #         service=service, origin=origin, destination=destination
        #     )
        #     .values("frequency")
        #     .order_by("frequency")
        #     .annotate(date=ArrayAgg("date", ordering="date"))
        #     .annotate(passengers=ArrayAgg("passengers"))
        # )
        timeseries = {}
        queryset = KTMBTimeseries.objects.filter(
            service=service, origin=origin, destination=destination
        ).order_by("frequency")

        for frequency, group in groupby(queryset, lambda x: x.frequency):
            timeseries[frequency] = dict()
            objects = sorted(
                list(group), key=lambda x: x.date
            )  # assure ordering (else returned not ordered)
            timeseries[frequency]["x"] = []
            timeseries[frequency]["passengers"] = []
            for object in objects:
                timeseries[frequency]["x"].append(unix_time_millis(object.date))
                timeseries[frequency]["passengers"].append(object.passengers)

        res = dict(
            data_as_of=ExplorersUpdate.objects.get(
                explorer=self.explorer_name, file_name="KTMBTimeseries"
            ).last_update,
            data=timeseries,
        )
        return res

    def get_timeseries_callout(self, service, origin, destination):
        callout = (
            KTMBTimeseriesCallout.objects.filter(
                service=service, origin=origin, destination=destination
            )
            .exclude(frequency="daily_7d")
            .values("frequency", "passengers")
            # .annotate(passengers=ArrayAgg("passengers"))
            .order_by("frequency")
        )
        callout_data = {}
        for object in callout.iterator():
            callout_data[object.get("frequency")] = object.get("passengers")

        return dict(
            data_as_of=ExplorersUpdate.objects.get(
                explorer=self.explorer_name, file_name="KTMBTimeseriesCallout"
            ).last_update,
            data=callout_data,
        )
