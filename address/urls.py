from django.urls import path
from address.views import AddressSearchByPostcodeView, AddressUploadView

urlpatterns = [
    path("", AddressUploadView.as_view(), name="address-upload"),
    path(
        "postcode/<str:postcode>",
        AddressSearchByPostcodeView.as_view(),
        name="address-search",
    ),
]
