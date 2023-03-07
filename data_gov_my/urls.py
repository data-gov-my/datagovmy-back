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
from django.urls import path
from data_gov_my import views

urlpatterns = [
    path("admin/", admin.site.urls),
    path("dashboard/", views.DASHBOARD.as_view(), name="DASHBOARD"),
    path("data-variable/", views.DATA_VARIABLE.as_view(), name="DATA_VARIABLE"),
    path("data-catalog/", views.DATA_CATALOG.as_view(), name="DATA_CATALOG"),
    path("update/", views.UPDATE.as_view(), name="UPDATE"),
    path("chart/", views.CHART.as_view(), name="CHART"),
]
