from django.urls import path
from .views import CommunityProductListCreateView, CommunityProductDetailView

urlpatterns = [
    path(
        "",
        CommunityProductListCreateView.as_view(),
        name="community-product-list",
    ),
    path(
        "list/",
        CommunityProductDetailView.as_view(),
        name="community-product-detail",
    ),
]
