import os
from random import randrange

from django.test import TestCase

from data_gov_my.models import Publication, Subscription
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from data_gov_my.utils.subscription_email_helper import SubscriptionEmail


class TestSubscriptionEmail(TestCase):
    def setUp(self):
        self.assertEqual(Publication.objects.count(), 0)
        builder = GeneralMetaBuilder.create(property="PUBLICATION")
        builder.build_operation(manual=True, rebuild="REBUILD", meta_files=[])
        self.assertGreater(Publication.objects.count(), 0)

        email = os.getenv('DJANGO_SUPERUSER_EMAIL')
        self.sub = Subscription.objects.create(email=email, publications=['all'])

    def test_send_email_en(self):
        pubs = Publication.objects.filter(language='en-GB')
        SubscriptionEmail(subscriber=self.sub, publication_id=pubs[randrange(len(pubs))].id).send_email()

    def test_send_email_bm(self):
        self.sub.language = 'ms-MY'
        self.sub.save()
        pubs = Publication.objects.filter(language='ms-MY')
        SubscriptionEmail(subscriber=self.sub, publication_id=pubs[randrange(len(pubs))].id).send_email()
