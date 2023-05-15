import os
import environ
from locust import HttpUser, task
from dotenv import load_dotenv

load_dotenv()


class User(HttpUser):
    def on_start(self):
        self.client.headers = {
            "Authorization": os.getenv("WORKFLOW_TOKEN"),
        }

    # Dashboards
    @task
    def exchange_rates(self):
        self.client.get(
            url="/dashboard", params={"dashboard": "exchange_rates", "state": "mys"}
        )

    @task
    def blood_donation(self):
        self.client.get(
            url="/dashboard", params={"dashboard": "blood_donation", "state": "mys"}
        )

    # Data Catalogues
    @task
    def geopoint(self):
        self.client.get(
            url="/data-variable", params={"id": "dgmy-public-mwe_mwe_geopoint_0"}
        )

    @task
    def table(self):
        self.client.get(
            url="/data-variable", params={"id": "dgmy-public-mwe_mwe_table_0"}
        )
