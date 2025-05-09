import os

from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend

import boto3
from post_office.models import Email


class SESAPIEmailBackend(BaseEmailBackend):
    def __init__(
            self,
            aws_access_key_id=os.getenv('AWS_SES_ACCESS_KEY_ID_DATA_REQUEST'),
            aws_secret_access_key=os.getenv('AWS_SES_SECRET_ACCESS_KEY_DATA_REQUEST'),
            region_name=settings.AWS_SES_REGION_NAME,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.client = boto3.client(
            'ses',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )
