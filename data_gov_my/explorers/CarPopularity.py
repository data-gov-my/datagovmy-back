from datetime import date, datetime
from itertools import groupby

import numpy as np
import pandas as pd
from django.contrib.postgres.search import TrigramSimilarity
from django.http import JsonResponse
from rest_framework import response

from data_gov_my.explorers.General import General_Explorer
from data_gov_my.models import Car, CarPopularityTimeseries


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
        Populate Car and CarPopularityTimeseries tables
        """
        if not source:
            raise ValueError(
                "There is no valid source URL for reading car popularity data!"
            )

        # Delete previous cars
        Car.objects.all().delete()

        df = pd.read_parquet(source)
        df = df.replace({np.nan: None})

        # Group DataFrame by unique combinations of maker and model
        grouped_data = df.groupby(["maker", "model"])

        # Create Car instances and populate CarPopularityTimeseries instances in bulk
        instances_to_create = []
        for (maker, model), group_df in grouped_data:
            # Create or get the Car instance
            car, created = Car.objects.get_or_create(maker=maker, model=model)
            # Create instances of CarPopularityTimeseries
            for _, row in group_df.iterrows():
                instances_to_create.append(
                    CarPopularityTimeseries(car=car, date=row["date"], cars=row["cars"])
                )
        # Bulk create instances of CarPopularityTimeseries
        CarPopularityTimeseries.objects.bulk_create(instances_to_create)

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
        cars = request_params.get("car", [])
        if not cars:
            return JsonResponse(
                {
                    "status": 400,
                    "message": f"Please provide either search query param, or >=1 car query params.",
                },
                status=400,
            )

        timeseries_data = self.get_timeseries(cars)

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
        queryset = (
            Car.objects.all()
            .annotate(similarity=TrigramSimilarity(search_type, query))
            .order_by("-similarity")[:limit]
        )

        return response.Response(queryset.values("id", "maker", "model", "similarity"))

    def get_timeseries(self, car_ids: list[str]):
        timeseries = dict(x=[])
        queryset = CarPopularityTimeseries.objects.filter(car__id__in=car_ids)
        x_completed = False
        for car, group in groupby(queryset, lambda x: x.car):
            # assure ordering (else returned not ordered)
            objects = sorted(list(group), key=lambda x: x.date)

            # we only want to populate x array once taking from the first car (rest are same)
            if not x_completed:
                for object in objects:
                    timeseries["x"].append(self.unix_time_millis(object.date))

            # compile queried car timeseries data
            timeseries[car.id] = dict(maker=car.maker, model=car.model, cars=[])
            for object in objects:
                timeseries[car.id]["cars"].append(object.cars)

            x_completed = True

        return timeseries
