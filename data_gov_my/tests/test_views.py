import hashlib
import os
from datetime import date

from django.core import mail
from django.test import override_settings
from django.urls import reverse
from django.utils import timezone
from jose import jwt
from rest_framework.test import APITestCase

from data_gov_my.models import AuthTable, PublicationType, PublicationSubtype, Subscription, Publication
from data_gov_my.utils.meta_builder import GeneralMetaBuilder
from data_gov_my.utils.publication_helpers import type_list, subtype_list, \
    populate_publication_types, populate_publication_subtypes, send_email_to_subscribers, craft_title, craft_template_en


class TestEmailSubscription(APITestCase):
    def setUp(self):
        self.test_email = 'test@gmail.com'
        self.another_email = 'test2@gmail.com'
        builder = GeneralMetaBuilder.create(property='PUBLICATION')
        builder.build_operation(manual=True, rebuild="REBUILD", meta_files=[])
        Subscription.objects.create(email=self.test_email, publications=[])
        Subscription.objects.create(email=self.another_email, publications=[])
        Subscription.objects.create(email='thisisarandomemail@yahoo.com', publications=[])
        self.assertEqual(Subscription.objects.count(), 3)

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        })
    def test_update_endpoint(self):
        # ensure that we have publication(s) to work on
        publications = Publication.objects.all()
        self.assertGreater(publications.count(), 0)

        # ensure that we have publication(s) released today, if not make one
        today = date.today()
        publications_today = Publication.objects.filter(
            release_date__year=today.year,
            release_date__month=today.month,
            release_date__day=today.day,
            language='en-GB'  # TODO: Get it dynamically
        )
        if not publications_today.count():
            mock_pub = publications[0]
            mock_pub.release_date = today
            mock_pub.save()

        publications_today = Publication.objects.filter(
            release_date__year=today.year,
            release_date__month=today.month,
            release_date__day=today.day,
            language='en-GB'  # TODO: Get it dynamically
        )

        for p in publications_today:
            # test first email
            subscription = Subscription.objects.get(email=self.test_email)
            subscription.publications.append(p.publication_type)
            subscription.save()

            # test second email
            subscription2 = Subscription.objects.get(email=self.another_email)
            subscription2.publications.append(p.publication_type)
            subscription2.save()

        publications_today_recount = Publication.objects.filter(
            release_date__year=today.year,
            release_date__month=today.month,
            release_date__day=today.day,
            language='en-GB')  # TODO: Get it dynamically
        # print(f'publications_today_recount: {publications_today_recount}')
        self.assertGreater(publications_today_recount.count(), 0)

        self.assertEqual(len(mail.outbox), 0)
        send_email_to_subscribers()
        self.assertEqual(len(mail.outbox), publications_today_recount.count()*2)
        subscriber_list = [s.email for s in Subscription.objects.all()]
        for o in mail.outbox:
            self.assertEqual(len(o.recipients()), 1)
            self.assertIn(o.recipients()[0], subscriber_list)
            # print(f'recipients: {o.to}')
            # print(f'subject: {o.subject}')
            # print(f'content: {o.body}')

class TestPreviewSubscriptionEmail(APITestCase):
    def setUp(self):
        self.test_email = 'test3@gmail.com'
        builder = GeneralMetaBuilder.create(property='PUBLICATION')
        builder.build_operation(manual=True, rebuild="REBUILD", meta_files=[])
        Subscription.objects.create(email=self.test_email, publications=[])
        self.assertEqual(Subscription.objects.count(), 1)

    @override_settings(
        POST_OFFICE={
            'BACKENDS': {'default': 'django.core.mail.backends.locmem.EmailBackend'},
            'DEFAULT_PRIORITY': 'now',
        })
    def test_preview_email(self):
        self.assertEqual(len(mail.outbox), 0)
        pub = Publication.objects.all()[0]
        sub = Subscription.objects.all()[0]

        subject = craft_title(pub.title)
        content = craft_template_en(pub.publication_id, pub.title, pub.description)
        from_email = 'notif@opendosm.my'
        recepient_list = [sub.email]
        print(
            f'Subject: {subject}\n'
            f'Content: {content}\n'
            f'From: {from_email}\n'
            f'Recipients: {recepient_list}\n'
        )
        mail.send_mail(
            subject,
            content,
            from_email,
            recepient_list,
            fail_silently=False,
        )
        self.assertEqual(len(mail.outbox), 1)

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
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()['message'], 'Email verified.')
        self.assertEqual(r.json()['email'], to)
        # print(r.json()['data'])

        # Get all subscriptions
        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url, headers={'token': token})
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
            headers={'token': token}
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
        r = self.client.get(url, headers={'token': token})
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
            headers={'token': token}
        )
        # print(r.json())
        self.assertEqual(r.status_code, 200)
        subs = Subscription.objects.get(email=to)
        self.assertEqual(
            len(subs.publications), 6
        )
        url = reverse('subscriptions')
        self.assertEqual(url, '/subscriptions/')
        r = self.client.get(url, headers={'token': token})
        for p in subs.publications:
            self.assertIn(p, r.json()['data'])
