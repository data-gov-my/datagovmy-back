import logging
from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, User
import numpy as np
import pandas as pd

from address.models import Address


class Command(BaseCommand):
    help = "Populate Address table with CSV file"

    def handle(self, *args, **options):
        # Check if the group exists, if not, create it
        df = pd.read_csv("address/management/commands/sample_klang_data.csv")
        df["combined_address"] = (
            df["unit"].fillna("")
            + " "
            + df["namaBangunan"].fillna("")
            + " "
            + df["namaJalan"].fillna("")
            + " "
            + df["lokaliti"]
            + " "
            + df["poskod"].astype(str)
            + " "
            + df["bandar"]
            + " "
            + df["negeri"]
            + " "
            + df["negara"]
        ).str.lower()
        df.replace({np.nan: None}, inplace=True)

        address_data = df.to_dict(orient="records")
        address_instances = [Address(**data) for data in address_data]
        Address.objects.all().delete()
        lst = Address.objects.bulk_create(address_instances, batch_size=20000)
        del address_data, address_instances
        logging.info(f"{len(lst)} addresses populated!")
