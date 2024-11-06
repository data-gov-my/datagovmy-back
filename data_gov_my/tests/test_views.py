import os
import json
import hashlib

from django.test import override_settings
from jose import jwt
from django.core import mail
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APITestCase

from data_gov_my.models import AuthTable, PublicationSubscription, PublicationType, PublicationSubtype, Subscription, \
    Publication
from data_gov_my.utils.publication_helpers import type_dict, subtype_dict, type_list, subtype_list, \
    populate_publication_types, populate_publication_subtypes


class TestEmailSubscribeSubmission(APITestCase):
    def setUp(self):

        # Populate PublicationType and PublicationSubtype
        populate_publication_types()
        self.assertEqual(PublicationType.objects.count(), len(type_list))

        populate_publication_subtypes()
        self.assertEqual(PublicationSubtype.objects.count(), len(subtype_list))

        # Generate SHA-256 hash
        data = "Your input data here"
        hash_object = hashlib.sha256(data.encode())
        hex_hash = hash_object.hexdigest()

        # print(hex_hash)
        self.value = f'Bearer {hex_hash}'
        AuthTable.objects.create(key='AUTH_TOKEN',
                                 value=self.value,
                                 timestamp=timezone.now()
                                 )

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        }
    )
    def test_full_subscription_flow(self):
        to = 'test@gmail.com'
        self.assertEqual(len(mail.outbox), 0)
        url = reverse("token_request")
        self.assertEqual(url, '/token/request/')
        r = self.client.post(url, {"email": to})
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mail.outbox), 1)
        # print(mail.outbox[0].subject)
        # print(f'mail body:{mail.outbox[0].body}')

        token = mail.outbox[0].body
        decoded_token = jwt.decode(token, os.getenv("WORKFLOW_TOKEN"))
        self.assertEqual(decoded_token['sub'], to)

        url = reverse("token_verify")
        self.assertEqual(url, '/token/verify/')
        r = self.client.post(url, {"token": token})
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], 'Email verified.')
        self.assertEqual(r.json()['email'], to)
        # print(r.json()['data'])

        # Get all subscriptions
        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url, headers={'Authorization': token})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['email'], to)
        # print(r.json())

        # Edit current subscriptions
        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.put(
            url,
            {
                'publications': [
                    'agriculture_supply_util',
                    'bci',
                    'bop',
                    'bop_annual_dia',
                    'bop_annual_fdi'
                ]
            },
            headers={'Authorization': token}
        )
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        subs = Subscription.objects.get(email=to)
        self.assertEqual(
            len(subs.publications), 5
        )
        for p in subs.publications:
            self.assertIn(p, [
            'agriculture_supply_util',
            'bci',
            'bop',
            'bop_annual_dia',
            'bop_annual_fdi'
        ])

        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url,  headers={'Authorization': token})
        # print(r.json())
        for p in subs.publications:
            self.assertIn(p, r.json()['data'])

        # Edit subscriptions second time
        r = self.client.put(
            url,
            {
                'publications': [
                    'trade_annual_sbh',
                    'trade_annual_services',
                    'trade_annual_state',
                    'trade_annual_swk',
                    'tradeindices',
                    'wrt'
                ]
            },
            headers={'Authorization': token}
        )
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        subs = Subscription.objects.get(email=to)
        self.assertEqual(
            len(subs.publications), 6
        )
        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url, headers={'Authorization': token})
        for p in subs.publications:
            self.assertIn(p, r.json()['data'])
