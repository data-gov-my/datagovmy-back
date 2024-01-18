import logging
from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, User
import numpy as np
import pandas as pd

from address.models import Address


class Command(BaseCommand):
    help = "Populate Address table with CSV file"
    PARQUET_LINK = "https://storage.data.gov.my/dashboards/alamat_sample.parquet"

    def handle(self, *args, **options):
        # Check if the group exists, if not, create it
        df = pd.read_parquet(self.PARQUET_LINK)
        df.replace({np.nan: None}, inplace=True)
        address_data = df.to_dict(orient="records")
        address_instances = [Address(**data) for data in address_data]
        Address.objects.all().delete()
        Address.objects.bulk_create(address_instances, batch_size=10000)
        logging.info(f"Created {Address.objects.count()} addresses.")
