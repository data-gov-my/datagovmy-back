from django.urls import path
from data_catalogue.views import DataCatalogueView

urlpatterns = [
    path("", DataCatalogueView.as_view(), name="data-catalogue-list"),
]
