from django.core import mail
from django.test import override_settings
from rest_framework.test import APITestCase
from django.urls import reverse

from data_gov_my.models import Subscription


class TestSubscriptionFlow(APITestCase):
    def setUp(self):
        pass

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        })
    def test_subscription_not_exists(self):
        # no email sent and no subscription
        self.assertEqual(len(mail.outbox), 0)
        self.assertEqual(Subscription.objects.count(), 0)

        email = 'test4@gmail.com'
        url = reverse('check-subscription')
        self.assertEqual(url, '/check-subscription/')
        r = self.client.post(url, data={'email': email})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], f'Email does not exist')

        # by this time, an email should be sent and a subscription should be created
        self.assertEqual(len(mail.outbox), 1)
        self.assertEqual(Subscription.objects.count(), 1)

        # try subscribe again
        r = self.client.post(url, data={'email': email})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], f'Email does exist')
