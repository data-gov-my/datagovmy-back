# Generated by Django 4.1.7 on 2023-07-20 06:09

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0036_explorersupdate"),
    ]

    operations = [
        migrations.RenameField(
            model_name="catalogjson",
            old_name="geographic",
            new_name="geography",
        ),
    ]
