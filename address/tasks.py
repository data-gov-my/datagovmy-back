import csv
import json
import os

import requests
from celery import shared_task
from .models import Address


@shared_task(name="Rebuild address")
def rebuild_address(file_content):
    # Create a CSV reader
    csv_reader = csv.reader(file_content.splitlines())

    # Assuming the first row of the CSV file contains column headers
    headers = next(csv_reader)

    # Create a list to store Address instances
    address_instances = []

    # Iterate through the remaining rows and create/update Address instances
    for row in csv_reader:
        address_data = dict(zip(headers, row))
        # Create an Address instance without saving it to the database
        address_instance = Address(**address_data)
        address_instances.append(address_instance)

    # Use bulk_create to efficiently create/update all instances in one query
    Address.objects.bulk_create(address_instances, batch_size=20000)
