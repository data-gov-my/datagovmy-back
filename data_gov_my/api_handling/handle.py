from django.http import QueryDict
from data_gov_my.models import CatalogJson, DashboardJson

"""
Build methods for any post-handling / additional info,
not covered by Meta Json
"""


def dashboard_additional_handling(params: QueryDict, res: dict):
    dashboard = params["dashboard"]

    if dashboard == "homepage":
        catalog_count = CatalogJson.objects.all().count()
        res["total_catalog"] = catalog_count
        return res
    elif dashboard == "kawasanku_admin":
        if params["area_type"] == "country":
            jitter_data = (
                DashboardJson.objects.filter(
                    dashboard_name="kawasanku_admin", chart_name="jitter_chart"
                )
                .values("chart_data")
                .first()
            )
            temp = jitter_data["chart_data"]["data"]["state"]
            res.setdefault("jitter_chart", {"data": None})
            res["jitter_chart"]["data"] = temp
            res["jitter_chart"]["data_as_of"] = jitter_data["chart_data"]["data_as_of"]
            return res
    return res
