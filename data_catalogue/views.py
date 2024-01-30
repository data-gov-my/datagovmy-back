from collections import defaultdict

from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.utils import translation
from rest_framework import generics, request, status
from rest_framework.response import Response
from rest_framework.views import APIView

from data_catalogue.models import DataCatalogueMeta, Dataviz, SiteCategory
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
        dataset_begin = request.query_params.get("begin", "")
        dataset_end = request.query_params.get("end", "")
        search = request.query_params.get("search", "")

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
        if dataset_begin and dataset_begin.isdigit():
            filters &= Q(dataset_begin__gte=int(dataset_begin))
        if dataset_end and dataset_end.isdigit():
            filters &= Q(dataset_end__lte=int(dataset_end))
        if search:
            filters &= Q(title__icontains=search) | Q(description__icontains=search)

        # Query the SiteCategory objects with related DataCatalogueMeta
        site_categories = (
            SiteCategory.objects.filter(site=site)
            .prefetch_related("datacataloguemeta_set")
            .order_by("category_sort", "category", "subcategory_sort", "subcategory")
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
            data_catalogue_metas = (
                site_category.datacataloguemeta_set.filter(filters)
                .values("id", "title", "data_as_of", "description", "data_source")
                .order_by("title_sort", "title")
            )

            # Append relevant DataCatalogueMeta ids to the nested dictionary
            for data_catalogue_meta in data_catalogue_metas:
                sitemap[category][subcategory].append(data_catalogue_meta)
                count += 1

        return Response(
            dict(total=count, source_filters=source_filters, dataset=sitemap)
        )


class DataCatalogueRetrieveAPIView(APIView):
    def get(self, request, *args, **kwargs):
        # get language
        language = request.query_params.get("language", "en")
        language = "ms" if language == "bm" else language
        if language not in ["en", "ms"]:
            return Response(
                {"error": "language param should be `en`, `ms` or `bm` only."},
                status=status.HTTP_400_BAD_REQUEST,
            )

        translation.activate(language)

        # table key should be a reserve (dataviz[0])
        dataviz_id = request.query_params.get("dataviz_id", "table")
        dataviz_object = get_object_or_404(
            Dataviz, catalogue_meta=kwargs.get("catalogue_id"), dataviz_id=dataviz_id
        )

        filter_columns = dataviz_object.config.get("filter_columns", [])
        dropdown = []
        can_be_filtered = True
        selected_or_default_filter_map = {}
        for i, filter_column in enumerate(filter_columns):
            if can_be_filtered:
                valid_options = [
                    str(item[filter_column])
                    for item in dataviz_object.dropdown
                    if all(
                        selected_or_default_filter_map.get(f"slug__{param}")
                        == str(item[param])
                        for param in filter_columns[:i]
                    )
                ]
            else:
                valid_options = []

            default = dataviz_object.dropdown[0][filter_column]
            filter_column_input = request.query_params.get(filter_column)
            selected = (
                filter_column_input if filter_column_input in valid_options else default
            )
            selected_or_default_filter_map[f"slug__{filter_column}"] = selected

            dropdown.append(
                {
                    "name": filter_column,
                    "selected": selected,
                    "options": list(dict.fromkeys(valid_options)),
                }
            )

        instance = get_object_or_404(DataCatalogueMeta, id=kwargs.get("catalogue_id"))
        data = instance.datacatalogue_set.filter(
            **selected_or_default_filter_map
        ).values_list("data", flat=True)
        serializer = DataCatalogueMetaSerializer(instance)
        res = serializer.data
        res["dropdown"] = dropdown
        res["data"] = data

        return Response(res)
