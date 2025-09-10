import os

import boto3
from django.conf import settings
from django.core.mail.backends.base import BaseEmailBackend
from botocore.exceptions import BotoCoreError, ClientError


class DataGovMYSESBackend(BaseEmailBackend):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = boto3.client(
            'ses',
            region_name=settings.AWS_SES_REGION_NAME,
            aws_access_key_id=settings.AWS_SES_ACCESS_KEY_ID_DATA_GOV_MY,
            aws_secret_access_key=settings.AWS_SES_SECRET_ACCESS_KEY_GOV_MY,
        )

    def send_messages(self, email_messages):
        num_sent = 0
        for message in email_messages:
            try:
                response = self.client.send_email(
                    Source=message.from_email,
                    Destination={
                        'ToAddresses': message.to,
                        'CcAddresses': message.cc,
                        'BccAddresses': message.bcc
                    },
                    Message={
                        'Subject': {'Data': message.subject},
                        'Body': {
                            'Text': {'Data': message.body},
                            'Html': {'Data': message.alternatives[0][0]} if message.alternatives else {
                                'Data': message.body}
                        }
                    }
                )
                num_sent += 1
            except (BotoCoreError, ClientError) as e:
                if not self.fail_silently:
                    raise
        return num_sent
