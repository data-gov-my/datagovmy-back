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
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], 'List of subscription returned.')
        self.assertEqual(r.json()['email'], to)
        self.assertEqual(type(r.json()['data']), list)
        # print(r.json()['data'])

        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url + f'?token={token}')
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['email'], to)
        # self.assertEqual(r.json()['data'], to)
        subscription = r.json()['data']
        for s in subscription:
            self.assertEqual(s['is_subscribed'], False)

        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.put(
            url,
            {'token': token,
             'publications': [
                 'agriculture_supply_util',
                 'bci',
                 'bop',
                 'bop_annual_dia',
                 'bop_annual_fdi'
             ]
             }
        )
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        subs = Subscription.objects.get(email=to)
        self.assertEqual(
            subs.publications.count(), 5
        )
        for pubs in subs.publications.all():
            self.assertIn(pubs.id, [
                'agriculture_supply_util',
                'bci',
                'bop',
                'bop_annual_dia',
                'bop_annual_fdi'
            ])

        r = self.client.put(
            url,
            {
                'token': token,
                'publications': [
                    'trade_annual_sbh',
                    'trade_annual_services',
                    'trade_annual_state',
                    'trade_annual_swk',
                    'tradeindices',
                    'wrt'
                ]
            }
        )
        self.assertEqual(r.status_code, 200)
        subs = Subscription.objects.get(email=to)
        self.assertEqual(
            subs.publications.count(), 6
        )
        for pubs in subs.publications.all():
            self.assertIn(pubs.id, [
                'trade_annual_sbh',
                'trade_annual_services',
                'trade_annual_state',
                'trade_annual_swk',
                'tradeindices',
                'wrt'
            ])
