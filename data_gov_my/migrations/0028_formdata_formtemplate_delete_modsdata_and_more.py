# Generated by Django 4.1.7 on 2023-05-26 03:31

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("post_office", "0011_models_help_text"),
        ("data_gov_my", "0027_catalogjson_demographic"),
    ]

    operations = [
        migrations.CreateModel(
            name="FormData",
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
                    "language",
                    models.CharField(
                        choices=[("en-GB", "English"), ("ms-MY", "Bahasa Melayu")],
                        default="en-GB",
                        max_length=5,
                    ),
                ),
                ("form_data", models.JSONField()),
                (
                    "email",
                    models.OneToOneField(
                        null=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        to="post_office.email",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="FormTemplate",
            fields=[
                (
                    "form_type",
                    models.CharField(max_length=50, primary_key=True, serialize=False),
                ),
                ("form_meta", models.JSONField()),
                (
                    "email_template",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="post_office.emailtemplate",
                    ),
                ),
            ],
        ),
        migrations.DeleteModel(
            name="ModsData",
        ),
        migrations.AddField(
            model_name="formdata",
            name="form_type",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                to="data_gov_my.formtemplate",
            ),
        ),
    ]
