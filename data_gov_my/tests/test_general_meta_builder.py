from django.test import TestCase

from data_gov_my.models import Publication
from data_gov_my.utils.meta_builder import GeneralMetaBuilder, PublicationBuilder


class TestEmailSubscription(TestCase):
    def setUp(self):
        # check publication
        print(Publication.objects.count())

        files = []
        GeneralMetaBuilder.selective_update()
        # builder = GeneralMetaBuilder.create() #property="PUBLICATION")
        # builder.build_operation(manual=True, rebuild="REBUILD", meta_files=files)

    def test_email_subscription(self):
        # check publication
        print(Publication.objects.count())
