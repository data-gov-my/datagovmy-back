from datetime import date, datetime
from itertools import groupby
from django.shortcuts import get_object_or_404

import numpy as np
import pandas as pd
from django.apps import apps
from django.http import JsonResponse
from rest_framework import response

from data_gov_my.models import ExplorersUpdate, MetaJson
from data_gov_my.utils.general_chart_helpers import STATE_ABBR


class General_Explorer:
    # General Data
    explorer_name = ""

    # Data Update
    data_update = ""
    columns_rename = {}
    columns_exclude = []

    # Data Populate
    batch_size = 10000
    data_populate = {}

    # API handling
    param_models = {}
    required_params = []

    def __init__(self):
        pass

    """
    Handles the API requests,
    and returns the data accordingly.
    """

    def handle_api(self, request_params):
        print("API Handling")

    """
    Clears the model specified.
    If none specified, it'll clear all models,
    related to this class.
    """

    def clear_db(self, model_name=""):
        if model_name:
            model_choice = apps.get_model("data_gov_my", model_name)
            model_choice.objects.all().delete()
        else:
            for k in list(self.data_populate.keys()):
                model_choice = apps.get_model("data_gov_my", k)
                model_choice.objects.all().delete()

    """
    Performs an update to the database.
    """

    def update(self, table_name="", unique_keys=[]):
        df = pd.read_parquet(self.data_populate[table_name])
        df = df.replace({np.nan: None})
        df = df.drop(columns=self.columns_exclude)

        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if self.columns_rename:
            df.rename(columns=self.columns_rename, inplace=True)

        model_choice = apps.get_model("data_gov_my", table_name)
        update_fields = list(set(df.columns.to_list()) - set(unique_keys))
        queryset = [model_choice(**x) for x in df.to_dict("records")]

        model_choice.objects.bulk_create(
            queryset,
            update_conflicts=True,
            unique_fields=unique_keys,
            update_fields=update_fields,
        )

    """
    Populates the table,
    assumes table is empty,
    pre-population.
    """

    def populate_db(self, table="", rebuild=False):
        if table:  # If set, builds only the table requested
            self.bulk_insert(
                self.data_populate[table],
                table,
                rebuild,
                self.batch_size,
                self.columns_rename,
                self.columns_exclude,
            )
        else:  # Builds all tables set in the data_populate attribute
            for k, v in self.data_populate.items():
                self.bulk_insert(
                    v,
                    k,
                    rebuild,
                    self.batch_size,
                    self.columns_rename,
                    self.columns_exclude,
                )

    """
    Allows bulk insert into models,
    for large datasets. Inserts by
    batches.
    """

    def bulk_insert(
        self,
        file,
        model_name,
        rebuild=False,
        batch_size=10000,
        rename_columns={},
        exclude=[],
    ):
        df = pd.read_parquet(file)
        df = df.replace({np.nan: None})
        df = df.drop(columns=exclude)

        if "state" in df.columns:
            df["state"].replace(STATE_ABBR, inplace=True)

        if rename_columns:
            df.rename(columns=rename_columns, inplace=True)

        groups = df.groupby(np.arange(len(df)) // batch_size)

        model_choice = apps.get_model("data_gov_my", model_name)

        if rebuild:
            model_choice.objects.all().delete()

        for k, v in groups:
            model_rows = [model_choice(**i) for i in v.to_dict("records")]
            model_choice.objects.bulk_create(model_rows)

    def get_last_update(self, model_name=""):
        obj = ExplorersUpdate.objects.filter(
            explorer=self.explorer_name, file_name=model_name
        ).first()
        if obj:
            return obj.last_update
        return None

    """
    Validates a request,
    by checking if all parameters exists.
    """

    def is_params_exist(self, request_params):
        for i in self.required_params:
            if i not in request_params:
                return False
        return True

    """
    Converts a string into a boolean
    """

    def str2bool(self, b):
        return b.lower() in ("yes", "true", "t", "1")


class GeneralTransportExplorer(General_Explorer):
    required_params = ["service", "origin", "destination"]
    EPOCH = datetime(1970, 1, 1)
    TIMESERIES_MODEL = None
    TIMESERIES_CALLOUT_MODEL = None

    @classmethod
    def unix_time_millis(cls, dt):
        if isinstance(dt, date):
            dt = datetime.combine(dt, datetime.min.time())
        return int((dt - cls.EPOCH).total_seconds() * 1000.0)

    def handle_api(self, request_params):
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

        data_last_updated = MetaJson.objects.get(
            dashboard_name=self.explorer_name
        ).dashboard_meta.get("data_last_updated", None)
        res = dict(
            data_last_updated=data_last_updated,
            timeseries=timeseries,
            timeseries_callout=callout,
        )
        return response.Response(res)

    def get_dropdown(self):
        queryset = (
            self.TIMESERIES_CALLOUT_MODEL.objects.order_by("service", "origin")
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
        timeseries = {}
        queryset = self.TIMESERIES_MODEL.objects.filter(
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
                timeseries[frequency]["x"].append(self.unix_time_millis(object.date))
                timeseries[frequency]["passengers"].append(object.passengers)

        res = dict(
            data_as_of=get_object_or_404(
                ExplorersUpdate,
                explorer=self.explorer_name,
                file_name=self.TIMESERIES_MODEL.__name__,
            ).last_update,
            data=timeseries,
        )
        return res

    def get_timeseries_callout(self, service, origin, destination):
        callout = (
            self.TIMESERIES_CALLOUT_MODEL.objects.filter(
                service=service, origin=origin, destination=destination
            )
            .values("frequency", "passengers")
            .order_by("frequency")
        )
        callout_data = {}
        for object in callout.iterator():
            callout_data[object.get("frequency")] = object.get("passengers")

        return dict(
            data_as_of=get_object_or_404(
                ExplorersUpdate,
                explorer=self.explorer_name,
                file_name=self.TIMESERIES_CALLOUT_MODEL.__name__,
            ).last_update,
            data=callout_data,
        )
