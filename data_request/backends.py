import os

import boto3
from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend


class DataRequestEmailBackend(BaseEmailBackend):
    def __init__(
            self,
            region_name=settings.AWS_SES_REGION_NAME,
            aws_access_key_id=settings.AWS_SES_ACCESS_KEY_ID_DATA_REQUEST,
            aws_secret_access_key=settings.AWS_SES_SECRET_ACCESS_KEY_DATA_REQUEST,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.client = boto3.client(
            'ses',
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
