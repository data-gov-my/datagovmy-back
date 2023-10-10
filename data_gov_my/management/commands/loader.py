import environ
from django.core.management.base import BaseCommand
from data_gov_my.utils.meta_builder import GeneralMetaBuilder

env = environ.Env()
environ.Env.read_env()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "operation", nargs="+", type=str, help="States what the operation should be"
        )

    def handle(self, *args, **kwargs):
        category = kwargs["operation"][0]
        operation = kwargs["operation"][1]

        if len(kwargs["operation"]) > 2:
            files = kwargs["operation"][2]
            files = files.split(",")
        else:
            files = []

        rebuild = operation == "REBUILD"

        """
        CATEGORIES :
        1. DATA_CATALOG
        2. DASHBOARD

        OPERATIONS :
        1. UPDATE
            - Updates the db, by updating values of pre-existing records

        2. REBUILD
            - Rebuilds the db, by clearing existing values, and inputting new ones

        SAMPLE COMMAND :
        - python manage.py loader DATA_CATALOG REBUILD
        - python manage.py loader DASHBOARDS UPDATE meta_1,meta_2
        """
        if category in [
            "DATA_CATALOG",
            "DATA_CATALOGUE",
            "DASHBOARDS",
            "I18N",
            "FORMS",
            "EXPLORERS",
            "PUBLICATION",
            "PUBLICATION_DOCS",
            "PUBLICATION_UPCOMING",
        ] and operation in [
            "UPDATE",
            "REBUILD",
        ]:
            if operation == "REBUILD" and category in [
                "PUBLICATION",
                "PUBLICATION_DOCS",
            ]:
                raise InterruptedError(
                    "REBUILD operation is not allowed for models that contain `download` field. Please delete the objects individually to avoid data loss!"
                )
            builder = GeneralMetaBuilder.create(property=category)
            builder.build_operation(manual=True, rebuild=rebuild, meta_files=files)
