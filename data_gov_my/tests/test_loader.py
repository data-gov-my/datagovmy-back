import environ
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from data_gov_my.models import (
    CatalogJson,
    DashboardJson,
    FormTemplate,
    MetaJson,
    i18nJson,
)
import pytest

env = environ.Env()
environ.Env.read_env()

# TODO: Model related tests should be done on a test DB
# - define a settings_test.py and point to it in pytest.ini
# - define separate .env variables for test
@pytest.mark.django_db
def test_meta_build_catalogue():
    category = "DATA_CATALOG"
    operation = "REBUILD"
    rebuild = operation == "REBUILD"
    files = []
    GeneralMetaBuilder.build_operation_by_category(
        manual=True, category=category, rebuild=rebuild, meta_files=files
    )
    # TODO: how to test the above operation is successful?
    assert CatalogJson.objects.count() > 0


# TODO: Add more test cases
