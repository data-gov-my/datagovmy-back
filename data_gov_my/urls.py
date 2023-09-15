"""Data Gov My URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.0/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from data_gov_my import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("auth-token/", views.AUTH_TOKEN.as_view(), name="AUTH_TOKEN"),
    path("dashboard/", views.DASHBOARD.as_view(), name="DASHBOARD"),
    path("data-variable/", views.DATA_VARIABLE.as_view(), name="DATA_VARIABLE"),
    path("data-catalog/", views.DATA_CATALOG.as_view(), name="DATA_CATALOG"),
    path("update/", views.UPDATE.as_view(), name="UPDATE"),
    path("chart/", views.CHART.as_view(), name="CHART"),
    path("dropdown/", views.DROPDOWN.as_view(), name="DROPDOWN"),
    path("explorer/", views.EXPLORER.as_view(), name="EXPLORER"),
    # path("i18n/", views.I18N.as_view(), name="I18N"),
    path("forms/<str:form_type>", views.FORMS.as_view(), name="FORMS"),
    path("view-count/", views.VIEW_COUNT.as_view(), name="VIEW_COUNT"),
    path(
        "update-view-count/",
        views.UPDATE_VIEW_COUNT.as_view(),
        name="UPDATE_VIEW_COUNT",
    ),
    path(
        "publication-dropdown/",
        views.PUBLICATION_DROPDOWN.as_view(),
        name="PUBLICATION_DROPDOWN",
    ),
    path("publication/", views.PUBLICATION.as_view(), name="PUBLICATION"),
    path(
        "publication-resource/<str:id>",
        views.PUBLICATION_RESOURCE.as_view(),
        name="PUBLICATION",
    ),
    path(
        "pub-docs/<str:doc_type>",
        views.PUBLICATION_DOCS.as_view(),
        name="PUBLICATION_DOCS",
    ),
    path(
        "pub-docs-resource/<str:id>",
        views.PUBLICATION_DOCS_RESOURCE.as_view(),
        name="PUBLICATION_DOCS_RESOURCE",
    ),
    path(
        "pub-upcoming/calendar/",
        views.PUBLICATION_UPCOMING_CALENDAR.as_view(),
        name="PUB_UPCOMING_CALENDAR",
    ),
    path(
        "pub-upcoming/list/",
        views.PUBLICATION_UPCOMING_LIST.as_view(),
        name="PUB_UPCOMING_LIST",
    ),
    path(
        "pub-upcoming-dropdown/",
        views.PUBLICATION_UPCOMING_DROPDOWN.as_view(),
        name="PUB_UPCOMING_DROPDOWN",
    ),
    path(
        "publication-resource/download/",
        views.publication_resource_download,
        name="PUB_RESOURCE_DL",
    ),
]

urlpatterns += [path("django-rq/", include("django_rq.urls"))]
