from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT

from data_gov_my.models import CatalogJson


def filter_options(param_list):
    default_params = {
        "period": "",
        "geography": [],
        "begin": "",
        "end": "",
        "source": [],
        "search": "",
    }

    for k, v in default_params.items():
        if k in param_list:
            if isinstance(v, str):
                default_params[k] = param_list[k][0]
            else:
                default_params[k] = param_list[k][0].split(",")

    # Check if parameters have been set, remove those which have not
    default_params = {k: v for k, v in default_params.items() if v}

    return default_params


def set_filter_cache():
    full_list = CatalogJson.objects.all().values(
        "id",
        "catalog_name",
        "catalog_category",
        "catalog_category_name",
        "catalog_subcategory_name",
        "time_range",
        "geography",
        "data_source",
        "dataset_begin",
        "dataset_end",
    )
    full_list = list(full_list)
    cache.set("search_cache", full_list)


def filter_cache(filter_list, full_list):
    final_list = []
    for obj in full_list:
        geo = 0
        source = 0
        begin = -1
        end = -1
        period = -1
        search = -1

        if "period" in filter_list:
            period = 1 if obj["time_range"] == filter_list["period"] else 0
        if "begin" in filter_list:
            begin = 1 if int(filter_list["begin"]) >= int(obj["dataset_begin"]) else 0
        if "end" in filter_list:
            end = 1 if int(filter_list["end"]) <= int(obj["dataset_end"]) else 0
        if "geography" in filter_list:
            for g in filter_list["geography"]:
                if obj["geography"].find(g) != -1:
                    geo = 1
                    break
        else:
            geo = -1
        if "source" in filter_list:
            for s in filter_list["source"]:
                if obj["data_source"].find(s) != -1:
                    source = 1
                    break
        else:
            source = -1
        if "search" in filter_list:
            search = 1 if filter_list["search"].lower() in obj["id"].lower() else 0

        final_bool = True
        for b in [geo, source, period, begin, end, search]:
            if b == 0:
                final_bool = False
                break

        if final_bool:
            final_list.append(obj)

    for obj in final_list:
        obj.pop("time_range", None)
        obj.pop("geography", None)
        obj.pop("data_source", None)
        obj.pop("dataset_begin", None)
        obj.pop("dataset_end", None)

    return final_list
