# Generated by Django 4.1.7 on 2024-11-06 01:27

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0086_alter_publicationsubtype_subtype_bm_and_more"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="subscription",
            name="language",
        ),
        migrations.RemoveField(
            model_name="subscription",
            name="publications",
        ),
        migrations.AddField(
            model_name="subscription",
            name="publications",
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.CharField(max_length=100), default=list, size=None
            ),
        ),
    ]