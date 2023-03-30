import calendar
from typing import List
from data_gov_my.models import MetaJson, DashboardJson, CatalogJson
from datetime import datetime, timedelta
'''
Build methods for any post-handling / additional info,
not covered by Meta Json
'''


def aggregate_sum(epochs: List[int], births: List[int], start=int, end=int, groupByDay=True):
    """
    aggregate different dates by day/month across years between [start, end]
    """
    FIST_VALID_DATE = datetime(1970, 1, 1)
    hasLeap = any(calendar.isleap(y) for y in range(start, end + 1))
    start, end = datetime(year=start, month=1, day=1), datetime(
        year=end, month=12, day=31)
    n = 365 + hasLeap if groupByDay else 12
    count = [0]*n

    for i, e in enumerate(epochs):
        date = FIST_VALID_DATE + timedelta(milliseconds=e)
        if start <= date <= end:
            if groupByDay:
                pos = date.timetuple().tm_yday + (hasLeap and not calendar.isleap(date.year)
                                                  and date.timetuple().tm_yday > 59)  # 59 = 1 March
            else:
                pos = date.month
            count[pos - 1] += births[i]

    return {"x": list(range(1, n+1)), "y": count}


def dashboard_additional_handling(params, res):
    dashboard = params['dashboard'][0]

    if dashboard == 'homepage':
        catalog_count = CatalogJson.objects.all().count()
        res['total_catalog'] = catalog_count
        return res
    if dashboard == 'kawasanku_admin':
        if params['area-type'][0] == 'country':
            temp = res['jitter_chart']['data']['state']
            res['jitter_chart']['data'] = temp
            return res
        return res
    if dashboard == "birthday_popularity":
        if {"start", "end", "groupByDay"} <= params.keys():
            res = aggregate_sum(epochs=res["timeseries"]["data"]["x"],
                                births=res["timeseries"]["data"]["births"],
                                start=int(params["start"][0]),
                                end=int(params["end"][0]),
                                groupByDay=params["groupByDay"][0] == "true")
        return res

    else:  # Default if no additions to make
        return res
