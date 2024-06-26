# Generated by Django 4.2.6 on 2024-02-22 06:58

from django.db import migrations, models
import django_better_admin_arrayfield.models.fields


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0015_agency_emails"),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="agency",
            options={"verbose_name_plural": "agencies"},
        ),
        migrations.AlterField(
            model_name="agency",
            name="emails",
            field=django_better_admin_arrayfield.models.fields.ArrayField(
                base_field=models.EmailField(max_length=254), size=None
            ),
        ),
    ]
