from django_rq import job
from django.core.cache import cache
from data_gov_my.models import ViewCount

from data_gov_my.serializers import ViewCountSerializer

VIEW_COUNT_CACHE_KEY = "viewcount"
MAX_CACHE_SIZE = 5


@job("high")
def increment_view_count(id, type, metric):
    # Get the object and increment the relevant count
    cached_viewcount_lst = cache.get(VIEW_COUNT_CACHE_KEY) or []
    viewcount_object = next(
        (object for object in cached_viewcount_lst if object.id == id),
        None,
    )

    if not viewcount_object:
        viewcount_object, created = ViewCount.objects.get_or_create(
            id=id,
            type=type,
        )
        # increment from viewcount_object found in DB
        setattr(viewcount_object, metric, getattr(viewcount_object, metric) + 1)
        cached_viewcount_lst.append(viewcount_object)
        if len(cached_viewcount_lst) > MAX_CACHE_SIZE:
            items = ViewCount.objects.bulk_update(
                objs=cached_viewcount_lst,
                fields=[
                    "all_time_view",
                    "download_csv",
                    "download_parquet",
                    "download_png",
                    "download_svg",
                ],
            )
            cached_viewcount_lst = []
    else:
        # increment count if found in cache
        setattr(viewcount_object, metric, getattr(viewcount_object, metric) + 1)

    cache.set(VIEW_COUNT_CACHE_KEY, cached_viewcount_lst, timeout=None)
    res = ViewCountSerializer(viewcount_object).data
    return res
