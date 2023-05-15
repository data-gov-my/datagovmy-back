import os
from locust import HttpUser, task


class User(HttpUser):
    def on_start(self):
        # FIXME: headers are still missing for the actual request sent
        self.client.headers = {
            "Authorization": os.getenv("WORKFLOW_TOKEN"),
            "Content-ype": "application/json",
        }

    # Dashboards
    @task
    def sekolahku(self):
        self.client.get(
            url="/dashboard", params={"dashboard": "sekolahku", "code": "ABA0001"}
        )

    def blood_donation(self):
        self.client.get(
            url="/dashboard", params={"dashboard": "blood-donation", "state": "mys"}
        )

    # Data Catalogues
    @task
    def geopoint(self):
        self.client.get(
            url="/data-variable", params={"id": "dgmy-public-mwe_mwe_geopoint_0"}
        )
