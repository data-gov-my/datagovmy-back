import os

import boto3
from django.conf import settings
from django.core.management.base import BaseCommand

from post_office import mail


class Command(BaseCommand):
    help = "Test email backend"

    def handle(self, *args, **options):
        recipients_env = os.environ.get("TEST_EMAIL_RECIPIENTS", "")
        recipients = tuple(
            email.strip() for email in recipients_env.split(",") if email.strip()
        )
        print(recipients)
        mail.send(
            recipients=recipients,
            subject="test default mail send",
            message="test default mail send",
        )
        mail.send(
            sender=settings.DATA_GOV_MY_FROM_EMAIL,
            recipients=recipients,
            subject="test data request",
            message="test data request",
            backend="datagovmy_ses",
        )
