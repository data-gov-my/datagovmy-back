from django.core.management.base import BaseCommand
from django.contrib.auth.models import Group, User


class Command(BaseCommand):
    help = "Assigns users to the Data Request Manager group"

    def handle(self, *args, **options):
        # Check if the group exists, if not, create it
        group, created = Group.objects.get_or_create(name="Data Request Manager")
        msg = (
            self.style.SUCCESS("Data Request Manager group is successfully created")
            if created
            else self.style.NOTICE(
                "Data Request Manager group has already been created."
            )
        )
        self.stdout.write(msg)
