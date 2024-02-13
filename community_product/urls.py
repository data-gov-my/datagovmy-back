from django.urls import path

from .views import (
    CommunityProductCreateView,
    CommunityProductDetailAPIView,
    CommunityProductListView,
)

urlpatterns = [
    path(
        "",
        CommunityProductCreateView.as_view(),
        name="community-product-create",
    ),
    path(
        "list/",
        CommunityProductListView.as_view(),
        name="community-product-list",
    ),
    path(
        "<int:id>",
        CommunityProductDetailAPIView.as_view(),
        name="community-product-detail",
    ),
]
