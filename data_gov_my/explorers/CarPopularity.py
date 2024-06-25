from datetime import date, datetime
from itertools import groupby

import pandas as pd
from django.apps import apps
from django.contrib.postgres.search import TrigramSimilarity
from django.db.models import CharField, Q, Value
from django.db.models.functions import Concat
from django.http import JsonResponse
from rest_framework import response

from data_gov_my.explorers.General import General_Explorer
from data_gov_my.models import (
    Car,
    CarPopularityTimeseriesMaker,
    CarPopularityTimeseriesModel,
)


class CarPopularityExplorer(General_Explorer):
    EPOCH = datetime(1970, 1, 1)
    explorer_name = "car_popularity"

    @classmethod
    def unix_time_millis(cls, dt):
        if isinstance(dt, date):
            dt = datetime.combine(dt, datetime.min.time())
        return int((dt - cls.EPOCH).total_seconds() * 1000.0)

    def populate_db(self, table="", source=None, rebuild=False):
        """
        Populate CarPopularityTimeseriesMaker and CarPopularityTimeseriesModel tables
        """
        # Read parquet file
        df = pd.read_parquet(source)
        model = apps.get_model("data_gov_my", table)

        # Bulk insert into the table
        if rebuild:
            model.objects.all().delete()

        res = model.objects.bulk_create(
            [model(**row) for row in df.to_dict(orient="records")],
            batch_size=self.batch_size,
        )

        # Update car table for fuzzy search
        if table == "CarPopularityTimeseriesModel":
            Car.objects.all().delete()
            df = df[["maker", "model"]].drop_duplicates()
            Car.objects.bulk_create(
                [
                    Car(
                        maker=row["maker"],
                        model=row["model"],
                        maker_model=f"{row['maker']} {row['model']}",
                    )
                    for _, row in df.iterrows()
                ],
                batch_size=self.batch_size,
            )

    def handle_api(self, request_params: dict):
        """
        Fuzzy search process query
        """
        # check if looking for similar cars
        maker = request_params.get("maker", [None])[0]
        model = request_params.get("model", [None])[0]
        if maker:
            return self.get_cars_by_fuzzy_search(query=maker)
        elif model:
            return self.get_cars_by_fuzzy_search(query=model, isMaker=False)

        # compile timeseries chart data based on car params
        maker_id = request_params.get("maker_id", [])
        model_id = request_params.get("model_id", [])
        if not maker_id and not model_id:
            return JsonResponse(
                {
                    "status": 400,
                    "message": f"Please provide either search query param, or >=1 car query params.",
                },
                status=400,
            )

        if maker_id and model_id:
            if len(maker_id) != len(model_id) or len(maker_id) > 3:
                return JsonResponse(
                    {
                        "status": 400,
                        "message": f"Please provide equal number of maker_id and model_id that are <= 3.",
                    },
                    status=400,
                )
            timeseries_data = self.get_timeseries(maker_id, model_id)
        elif maker_id:
            if len(maker_id) > 3:
                return JsonResponse(
                    {
                        "status": 400,
                        "message": f"Please provide <=3 maker_id.",
                    },
                    status=400,
                )
            timeseries_data = self.get_timeseries(maker_id)

        data_last_updated, data_next_update = self.get_last_update_and_next_update(
            self.explorer_name
        )

        res = dict(
            data_last_updated=data_last_updated,
            data_next_update=data_next_update,
            timeseries=timeseries_data,
        )

        return response.Response(res)

    # 1527, 1555, 1551
    def get_cars_by_fuzzy_search(
        self, query: str = None, isMaker=True, limit: int = 10
    ):
        search_type = "maker" if isMaker else "model"
        if isMaker:

            queryset = (
                CarPopularityTimeseriesMaker.objects.all()
                .annotate(similarity=TrigramSimilarity(search_type, query))
                .distinct()
                .order_by("-similarity")[:limit]
                .values("maker", "similarity")
            )
        else:
            queryset = (
                Car.objects.annotate(similarity=TrigramSimilarity("maker_model", query))
                .order_by("-similarity")[:limit]
                .values("maker", "model", "similarity")
            )

        return response.Response(queryset)

    def get_timeseries(self, maker_ids: list[str], model_ids: list[str] = None):

        timeseries = dict(
            x=[],
        )
        if maker_ids and model_ids:
            queryset = CarPopularityTimeseriesModel.objects.all()
            filter = Q()
            for maker_id, model_id in zip(maker_ids, model_ids):
                filter |= Q(maker=maker_id, model=model_id)
            queryset = queryset.filter(filter)
            x_completed = False
            for car, group in groupby(queryset, lambda x: (x.maker, x.model)):
                maker, model = car
                # assure ordering (else returned not ordered)
                objects = sorted(list(group), key=lambda x: x.date)

                # we only want to populate x array once taking from the first car (rest are same)
                if not x_completed:
                    for object in objects:
                        timeseries["x"].append(self.unix_time_millis(object.date))

                # compile queried car timeseries data
                timeseries[f"{maker} {model}"] = dict(
                    maker=maker, model=model, cars=[], cars_cumul=[]
                )
                for object in objects:
                    timeseries[f"{maker} {model}"]["cars"].append(object.cars)
                    timeseries[f"{maker} {model}"]["cars_cumul"].append(
                        object.cars_cumul
                    )

                x_completed = True
        else:  # only makers
            queryset = CarPopularityTimeseriesMaker.objects.filter(maker__in=maker_ids)
            x_completed = False
            for car, group in groupby(queryset, lambda x: (x.maker)):
                # assure ordering (else returned not ordered)
                objects = sorted(list(group), key=lambda x: x.date)

                # we only want to populate x array once taking from the first car (rest are same)
                if not x_completed:
                    for object in objects:
                        timeseries["x"].append(self.unix_time_millis(object.date))

                # compile queried car timeseries data
                timeseries[car] = dict(maker=car, cars=[], cars_cumul=[])
                for object in objects:
                    timeseries[object.maker]["cars"].append(object.cars)
                    timeseries[object.maker]["cars_cumul"].append(object.cars_cumul)

                x_completed = True
        return timeseries
