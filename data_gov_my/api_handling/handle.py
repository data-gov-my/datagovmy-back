import calendar
from typing import List
from data_gov_my.models import MetaJson, DashboardJson, CatalogJson
from datetime import datetime, timedelta
'''
Build methods for any post-handling / additional info,
not covered by Meta Json
'''


def dates_in_year(year: int):
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


def dates_in_month():
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


def aggregate_sum(epochs: List[int], births: List[int], start=int, end=int, groupByDay=True):
    """
    Aggregate different dates by day across years between [start, end]
    """
    FIST_VALID_DATE = datetime(1970, 1, 1)
    hasLeap = any(calendar.isleap(y) for y in range(start, end + 1))
    start, end = datetime(year=start, month=1, day=1), datetime(
        year=end, month=12, day=31)
    count = [0]*(365 + hasLeap) if groupByDay else [0]*12

    for i, e in enumerate(epochs):
        date = FIST_VALID_DATE + timedelta(milliseconds=e)
        if start <= date <= end:
            pos = date.timetuple().tm_yday + (hasLeap and not calendar.isleap(date.year)
                                              and date.timetuple().tm_yday > 59) if groupByDay else date.month  # 59 = 1 March
            count[pos - 1] += births[i]

    if groupByDay:
        valid_dates = dates_in_year(1972) if hasLeap else dates_in_year(1970)
    else:
        valid_dates = dates_in_month()

    return {"x": valid_dates, "y": count}


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
