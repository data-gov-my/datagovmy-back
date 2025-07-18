# Generated by Django 4.1.7 on 2025-06-12 03:40

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0016_alter_agency_options_alter_agency_emails"),
    ]

    operations = [
        migrations.CreateModel(
            name="DataRequestAdminEmail",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "email",
                    models.EmailField(
                        help_text="Email address of the data request administrator",
                        max_length=254,
                        unique=True,
                    ),
                ),
                ("added_at", models.DateTimeField(auto_now_add=True)),
            ],
            options={
                "verbose_name": "Data Request Admin Email",
                "verbose_name_plural": "Data Request Admin Emails",
            },
        ),
    ]
