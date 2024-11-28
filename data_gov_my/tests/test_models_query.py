from django.test import TestCase
from faker import Faker

from data_gov_my.models import Subscription
from data_gov_my.utils.publication_helpers import subtype_list

faker = Faker()


class TestModelsQuery(TestCase):
    def setUp(self):
        pass

    def test_models_accuracy(self):
        # check to ensure no 'all' key in subtype_list
        self.assertNotIn('all', subtype_list)

        for s in subtype_list:
            Subscription.objects.create(email=faker.ascii_email(), publications=[s])
        all_email = faker.ascii_email()
        Subscription.objects.create(email=all_email, publications=['all'])

        for s in subtype_list:
            self.assertEqual(Subscription.objects.filter(publications__overlap=[s, 'all']).count(), 2)
