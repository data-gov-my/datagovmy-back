import json
import os

import requests
from celery import shared_task


@shared_task(name="POST TinyBird API Usage")
def post_api_log_to_tinybird(payload):
    data = json.dumps(payload)

    return requests.post(
        "https://api.tinybird.co/v0/events",
        params={
            "name": os.getenv("TINYBIRD_EVENTS_NAME"),
            "token": os.getenv("TINYBIRD_EVENTS_API_TOKEN"),
        },
        data=data,
    ).status_code
