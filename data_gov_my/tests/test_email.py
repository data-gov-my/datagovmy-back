import os
from random import randint

from django.core import mail
from django.test import TestCase, override_settings

from data_gov_my.models import Publication, Subscription
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from data_gov_my.utils.subscription_email_helper import SubscriptionEmail
from data_gov_my.utils.token_email_helper import TokenEmail


class TestTokenEmail(TestCase):
    def setUp(self):
        email = os.getenv('DJANGO_SUPERUSER_EMAIL')
        self.sub = Subscription.objects.create(email=email)

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        })
    def test_send_token_email_bm(self):
        self.assertEqual(len(mail.outbox), 0)
        self.sub.language = 'ms-MY'
        self.sub.save()
        t = TokenEmail(self.sub.email, self.sub.language)
        t.send_email()
        self.assertEqual(len(mail.outbox), 1)
        self.assertIn(t.get_token(), mail.outbox[0].body)
        # print(mail.outbox[0].body)

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        })
    def test_send_token_email_en(self):
        self.assertEqual(len(mail.outbox), 0)
        t = TokenEmail(self.sub.email)
        t.send_email()
        self.assertEqual(len(mail.outbox), 1)
        self.assertIn(t.get_token(), mail.outbox[0].body)
        # print(mail.outbox[0].body)

class TestSubscriptionEmail(TestCase):
    """
    Note: Be careful in running this test because it will send a real email to your mailbox
    """
    def setUp(self):
        self.assertEqual(Publication.objects.count(), 0)
        builder = GeneralMetaBuilder.create(property="PUBLICATION")
        builder.build_operation(manual=True, rebuild="REBUILD", meta_files=[])
        self.assertGreater(Publication.objects.count(), 0)
        # print(f'no of publications: {Publication.objects.count()}')

        email = os.getenv('DJANGO_SUPERUSER_EMAIL')
        self.sub = Subscription.objects.create(email=email, publications=['all'])

    def test_send_pub_email_en_and_bm_with_description_email(self):
        # pick one in en-GB with description_email
        pubs = Publication.objects.filter(language='en-GB', description_email__isnull=False)
        pub = pubs[randint(0, len(pubs)-1)]
        SubscriptionEmail(subscriber=self.sub, publication_id=pub.publication_id).send_email()

        # change locale
        self.sub.language = 'ms-MY'
        self.sub.save()
        pubs = Publication.objects.filter(language='ms-MY', description_email__isnull=False)
        pub = pubs[randint(0, len(pubs)-1)]
        SubscriptionEmail(subscriber=self.sub, publication_id=pub.publication_id).send_email()

        # pick one in en-GB without description_email
        pubs = Publication.objects.filter(language='en-GB', description_email__isnull=True)
        pub = pubs[randint(0, len(pubs)-1)]
        SubscriptionEmail(subscriber=self.sub, publication_id=pub.publication_id).send_email()

        # change locale
        self.sub.language = 'ms-MY'
        self.sub.save()
        pubs = Publication.objects.filter(language='ms-MY', description_email__isnull=True)
        pub = pubs[randint(0, len(pubs)-1)]
        SubscriptionEmail(subscriber=self.sub, publication_id=pub.publication_id).send_email()

