import logging
import environ
from django.core.management.base import BaseCommand
import pandas as pd
from data_gov_my.models import ViewCount
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from django.core.cache import caches

env = environ.Env()
environ.Env.read_env()


class Command(BaseCommand):
    def handle(self, *args, **kwargs):
        # FIXME temporarily stored in catalog_utils - remove CSV later
        df = pd.read_csv("data_gov_my/catalog_utils/viewcounts_consolidated.csv")
        logging.info(f"Found {df.count()} views")
        cache = caches["viewcount"]
        deleted_from_cache = 0
        for row in df.to_dict(orient="records"):
            id = row.pop("id")
            logging.info(f"Processing ID: {id}")
            # previously suffixed with _{i} - we'll delete them from cache
            found_keys = cache.keys(f"{id}_*")
            for key in found_keys:
                cache.delete(key)
                logging.info(f"Deleted {key} from cache...")

            created, isNew = ViewCount.objects.update_or_create(id=id, defaults=row)
            logging.info(f"Added to DB: {created} (isNew: {isNew})")
