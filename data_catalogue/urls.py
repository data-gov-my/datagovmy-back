from django.urls import path
from data_catalogue.views import DataCatalogueListAPIView, DataCatalogueRetrieveAPIView

urlpatterns = [
    path("", DataCatalogueListAPIView.as_view(), name="data-catalogue-list"),
    path(
        "<str:catalogue_id>",
        DataCatalogueRetrieveAPIView.as_view(),
        name="data-catalogue-item",
    ),
]
