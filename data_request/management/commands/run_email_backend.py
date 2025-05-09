import os

import boto3
from django.conf import settings
from django.core.management.base import BaseCommand

from post_office import mail

class Command(BaseCommand):
    help = "Test email backend"

    def handle(self, *args, **options):
        mail.send(
            recipients=('juwaini@gmail.com'),
            subject='test default mail send',
            message='test default mail send',
        )
        mail.send(
            sender=settings.DEFAULT_FROM_EMAIL_DATA_REQUEST,
            recipients=('juwaini@gmail.com'),
            subject='test data request',
            message='test data request',
            backend='data_request',
        )
