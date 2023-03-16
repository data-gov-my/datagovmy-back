from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache
from django.conf import settings
from django.core.cache.backends.base import DEFAULT_TIMEOUT
from data_gov_my.utils import cron_utils
from data_gov_my.catalog_utils import catalog_builder

from django.core.cache import cache

import os
import shutil
import environ
import importlib
import pandas as pd
import numpy as np
import time

from django.db import models
from django.db import connection
from django.apps import apps
from django.utils.module_loading import import_module
from django.core.management import call_command
from django.urls import clear_url_caches
from django.contrib import admin
from importlib import import_module,reload

env = environ.Env()
environ.Env.read_env()


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "operation", nargs="+", type=str, help="States what the operation should be"
        )

    def handle(self, *args, **kwargs):
        if kwargs['operation'][0] == "POPULATE" :
            dataset = kwargs['operation'][1]

            choice = {
                'FIRST_NAME' : {
                    "file" : "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/name_popularity_first.parquet",
                    "model" : "NameDashboard_FirstName"
                },
                'LAST_NAME' : {
                    "file" : "https://dgmy-public-dashboards.s3.ap-southeast-1.amazonaws.com/name_popularity_last.parquet",
                    "model" : "NameDashboard_LastName"
                },                
            }

            batch_size = 10000
            rename_columns = {}
            for i in range(1920, 2020, 10) :
                rename_columns[ f"{ str(i) }s" ] = f"d_{ str(i) }"            
            
            start_time = time.time()
            df = pd.read_parquet(choice[dataset]['file'])
            if rename_columns : 
                df.rename(columns = rename_columns, inplace = True)
            groups = df.groupby(np.arange(len(df))//batch_size)        
            
            model_choice = apps.get_model("data_gov_my", choice[dataset]['model'])
            for k,v in groups :
                model_rows = [ model_choice(**i) for i in v.to_dict('records') ]
                model_choice.objects.bulk_create(model_rows)            
            print(time.time() - start_time)
        
        else :

            category = kwargs["operation"][0]
            operation = kwargs["operation"][1]
            command = operation

            if len(kwargs["operation"]) > 2:
                files = kwargs["operation"][2]
                command = operation + " " + files

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

            if category in ["DATA_CATALOG", "DASHBOARDS"] and operation in [
                "UPDATE",
                "REBUILD",
            ]:
                # Delete all file src
                # os.remove("repo.zip")
                # shutil.rmtree("DATAGOVMY_SRC/")
                cron_utils.remove_src_folders()
                if category == "DATA_CATALOG":
                    catalog_builder.catalog_operation(command, "MANUAL")
                else:
                    cron_utils.data_operation(command, "MANUAL")
