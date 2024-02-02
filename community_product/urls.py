from django.urls import path
from .views import CommunityProductCreateView, CommunityProductDetailView

urlpatterns = [
    path(
        "",
        CommunityProductCreateView.as_view(),
        name="community-product-list",
    ),
    path(
        "list/",
        CommunityProductDetailView.as_view(),
        name="community-product-detail",
    ),
]
