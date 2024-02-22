from django.urls import path

from data_request.views import (
    AgencyCreateAPIView,
    AgencyListAPIView,
    DataRequestCreateAPIView,
    SubscriptionCreateAPIView,
    list_data_request,
)

urlpatterns = [
    path("", DataRequestCreateAPIView.as_view(), name="data-request-create"),
    path("list/", list_data_request, name="data-request-list"),
    path(
        "<int:ticket_id>/subscription/",
        SubscriptionCreateAPIView.as_view(),
        name="data-request-subscribe",
    ),
    path(
        "agencies/",
        AgencyCreateAPIView.as_view(),
        name="agency-create",
    ),
    path(
        "agencies/list",
        AgencyListAPIView.as_view(),
        name="agency-list",
    ),
]
