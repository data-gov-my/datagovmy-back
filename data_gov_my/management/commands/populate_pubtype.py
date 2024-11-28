import requests
import pandas as pd
from io import BytesIO

from django.core.management.base import BaseCommand

from data_gov_my.models import PublicationType, PublicationSubtype
from data_gov_my.utils.meta_builder import PublicationTypeBuilder


class Command(BaseCommand):
    def handle(self, *args, **options):
        PublicationType.objects.all().delete()

        r = requests.get('https://storage.dosm.gov.my/meta/arc_types.parquet')
        parquet_file = BytesIO(r.content)
        df = pd.read_parquet(parquet_file)
        type_list = df['type'].unique().tolist()

        for l in type_list:
            PublicationType.objects.create(id=l, dict_bm={}, dict_en={})

        assert len(type_list) == PublicationType.objects.count()

        for index, row in df.iterrows():
            p = PublicationType.objects.get(id=row['type'])
            p.type_en = row['type_en']
            p.type_bm = row['type_bm']
            p.dict_bm[row['subtype']] = row['subtype_bm']
            p.dict_en[row['subtype']] = row['subtype_en']
            p.save()
