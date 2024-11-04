from django.core.management.base import BaseCommand

from data_gov_my.models import Subscription, PublicationSubtype
from data_gov_my.utils.publication_helpers import populate_publication_types, populate_publication_subtypes

class Command(BaseCommand):
    def handle(self, *args, **options):
        populate_publication_types()
        populate_publication_subtypes()

        subs, created = Subscription.objects.get_or_create(
            email='test@gmail.com'
        )

        publication_list = [
            'agriculture_supply_util',
            'bci',
            'bop',
            'bop_annual_dia',
            'bop_annual_fdi'
        ]
        pst_list = [PublicationSubtype.objects.get(id=p) for p in publication_list]
        for pst in pst_list:
            subs.publications.add(pst)
