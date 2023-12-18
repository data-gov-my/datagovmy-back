from django.shortcuts import get_object_or_404, render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import generics, status, request
from django.utils import translation

from data_catalogue.models import DataCatalogueMeta, SiteCategory
from collections import defaultdict
from django.db.models import Q

from data_catalogue.serializers import DataCatalogueMetaSerializer


# Create your views here.
class DataCatalogueListAPIView(APIView):
    def get(self, request: request.Request):
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

        # Extract additional query parameters
        data_source = request.query_params.getlist("source", [])
        frequency = request.query_params.get("frequency", "")
        geography = request.query_params.getlist("geography", [])
        demography = request.query_params.getlist("demography", [])
        dataset_begin = request.query_params.get("dataset_begin", "")
        dataset_end = request.query_params.get("dataset_end", "")

        # Prepare filter conditions based on query parameters
        filters = Q()
        if data_source:
            filters &= Q(data_source__contains=data_source)
        if frequency:
            filters &= Q(frequency=frequency)
        if geography:
            filters &= Q(geography__overlap=geography)
        if demography:
            filters &= Q(demography__overlap=demography)
        if dataset_begin:
            filters &= Q(dataset_begin__gte=dataset_begin)
        if dataset_end:
            filters &= Q(dataset_end__lte=dataset_end)

        # Query the SiteCategory objects with related DataCatalogueMeta
        site_categories = (
            SiteCategory.objects.filter(site=site)
            .prefetch_related("datacataloguemeta_set")
            .order_by("category_en", "subcategory_en")
        )

        # Initialize nested dictionary and set
        source_filters = DataCatalogueMeta.objects.filter(
            site_category__in=site_categories
        ).values_list("data_source", flat=True)
        source_filters = list(set().union(*source_filters))
        source_filters.sort()
        sitemap = defaultdict(lambda: defaultdict(list))
        count = 0

        # Iterate through SiteCategory instances and populate the nested dictionary
        for site_category in site_categories:
            category = site_category.category
            subcategory = site_category.subcategory
            data_catalogue_metas = site_category.datacataloguemeta_set.filter(
                filters
            ).values("id", "title", "data_as_of", "description", "data_source")

            # Append relevant DataCatalogueMeta ids to the nested dictionary
            for data_catalogue_meta in data_catalogue_metas:
                sitemap[category][subcategory].append(data_catalogue_meta)
                count += 1

        return Response(
            dict(total=count, source_filters=source_filters, dataset=sitemap)
        )


class DataCatalogueRetrieveAPIView(APIView):
    def get(self, request: request.Request, *args, **kwargs):
        # get language
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        if language not in ["en", "ms"]:
            return Response(
                {"error": "language param should be `en`, `ms` or `bm` only."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        translation.activate(language)

        catalogue_id = kwargs.get("catalogue_id")
        dc_meta = get_object_or_404(DataCatalogueMeta, id=catalogue_id)
        return Response({"lang": language, "data": dc_meta.methodology})
