from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, status
from django.utils import translation

from data_catalogue.models import DataCatalogueMeta, SiteCategory
from collections import defaultdict


# Create your views here.
class DataCatalogueView(APIView):
    def get(self, request):
        # get site
        site = request.query_params.get("site", "datagovmy")
        if site not in ["datagovmy", "kkmnow", "opendosm"]:
            return Response(
                data={
                    "error": "query param `site` must be `datagovmy`, `kkmnow` or `opendosm`."
                },
                status=400,
            )

        # get language
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        if language not in ["en", "ms"]:
            return Response(
                {"error": "language param should be `en`, `ms` or `bm` only."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        translation.activate(language)

        # Query the SiteCategory objects with related DataCatalogueMeta
        site_categories = SiteCategory.objects.filter(site=site).prefetch_related(
            "datacataloguemeta_set"
        )

        # Initialize nested dictionary and set
        source_filters = set()
        sitemap = defaultdict(lambda: defaultdict(list))

        # Iterate through SiteCategory instances and populate the nested dictionary
        for site_category in site_categories:
            category = site_category.category
            subcategory = site_category.subcategory
            data_catalogue_metas = site_category.datacataloguemeta_set.values(
                "id", "title", "data_as_of", "description", "data_source"
            )

            # Append relevant DataCatalogueMeta ids to the nested dictionary
            for data_catalogue_meta in data_catalogue_metas:
                sitemap[category][subcategory].append(data_catalogue_meta)

            # continue compiling data source for general index page filters
            source_filters.update(data_catalogue_meta["data_source"])

        return Response(dict(source_filters=source_filters, dataset=sitemap))
