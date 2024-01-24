from django.urls import path
from address.views import AddressSearchView, AddressUploadView

urlpatterns = [
    path("", AddressUploadView.as_view(), name="address-upload"),
    path(
        "search/",
        AddressSearchView.as_view(),
        name="address-search",
    ),
]
