import os
import json
import hashlib

from django.test import override_settings
from jose import jwt
from django.core import mail
from django.urls import reverse
from django.utils import timezone
from rest_framework.test import APITestCase

from data_gov_my.models import AuthTable, PublicationSubscription


class TestEmailSubscribeSubmission(APITestCase):
    def setUp(self):
        subtype_list = [
            'agriculture_supply_util', 'bci', 'bop', 'bop_annual_dia', 'bop_annual_fdi',
            'businesses', 'census_mukim', 'census_economy', 'census_economy_accom',
            'census_economy_admin', 'census_economy_arts', 'census_economy_bumi',
            'census_economy_education', 'census_economy_electricity',
            'census_economy_employees', 'census_economy_establishment',
            'census_economy_finance', 'census_economy_fnb', 'census_economy_foreign',
            'census_economy_health', 'census_economy_ict', 'census_economy_infocomm',
            'census_economy_personal', 'census_economy_professional',
            'census_economy_property', 'census_economy_sector', 'census_economy_sme',
            'census_economy_transport', 'census_economy_water', 'census_economy_women',
            'census_economy_wrt', 'census_economy_youth', 'construction', 'cpi', 'crime',
            'bumi', 'children', 'cod', 'demography', 'matrimony', 'population', 'pwd',
            'vitalstatistics', 'women', 'capstock', 'digitalecon', 'gdp', 'gdp_advance',
            'gdp_district', 'gdp_income', 'gdp_state', 'gfcf', 'ictsa', 'msme', 'nea',
            'sports_sa', 'tourism', 'icths', 'iip', 'bts', 'mei', 'ipi', 'employment',
            'formal_wages', 'graduates', 'jobs', 'lfs', 'lfs_informal', 'productivity',
            'wages', 'lifetables', 'mfg', 'mfg_util', 'mining_png', 'mywi', 'osh', 'ppi',
            'sppi', 'rubber', 'sdg', 'services', 'sa_matrix', 'special_floods',
            'mydistrict', 'socioecon_state', 'handbook', 'lmr', 'mesr', 'msb',
            'pocketstats', 'social_bulletin', 'social_review', 'yearbook',
            'yearbook_sabah', 'yearbook_sarawak', 'fats', 'tourism_domestic',
            'tourism_domestic_state', 'trade', 'trade_annual_sbh',
            'trade_annual_services', 'trade_annual_state', 'trade_annual_swk',
            'tradeindices', 'wrt'
        ]

        for subtype in subtype_list:
            if len(subtype) > 50:
                print(subtype)
            PublicationSubscription.objects.create(publication_type=subtype)


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

    def test_send_email_subscription(self):
        test_email = 'test@gmail.com' # TODO: change using dynamic email

        url = reverse('send_email_subscription')
        r = self.client.post(url, {'email': test_email})
        # print(r.json())
        self.assertEqual(r.status_code, 200)

        self.assertEqual(PublicationSubscription.objects.filter(emails__contains=[test_email]).count(), 0)
        url = reverse('publication_subscribe')
        r = self.client.post(
            url,
            {
                'email': test_email,
                'publication_type': [
                    'agriculture_supply_util',
                    'bci',
                    'bop',
                    'bop_annual_dia',
                    'bop_annual_fdi'
                ]
            }
        )
        # print(r.json())
        self.assertEqual(r.status_code, 201)
        self.assertEqual(
            PublicationSubscription.objects.filter(emails__contains=[test_email]).count(), 5
        )

        r = self.client.post(
            url,
            {
                'email': test_email,
                'publication_type': [
                    'trade_annual_sbh',
                    'trade_annual_services',
                    'trade_annual_state',
                    'trade_annual_swk',
                    'tradeindices',
                    'wrt'
                ]
            }
        )
        self.assertEqual(r.status_code, 201)
        self.assertEqual(PublicationSubscription.objects.filter(emails__contains=[test_email]).count(), 6)

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        }
    )
    def test_get_login_email(self):
        to = 'test@gmail.com'
        self.assertEqual(len(mail.outbox), 0)
        r = self.client.post(reverse("send_email_login"), {"email": to})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(mail.outbox), 1)
        # print(mail.outbox[0].subject)
        # print(f'mail body:{mail.outbox[0].body}')

        decoded_token = jwt.decode(mail.outbox[0].body, os.getenv("WORKFLOW_TOKEN"))
        self.assertEqual(decoded_token['sub'], to)

        r = self.client.post(reverse("validate_email"), {"token": mail.outbox[0].body})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], 'List of subscription returned.')
        self.assertEqual(type(r.json()['data']), list)
        # print(r.json()['data'])