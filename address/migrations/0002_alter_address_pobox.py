# Generated by Django 4.1.7 on 2024-01-17 03:29

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("address", "0001_initial"),
    ]

    operations = [
        migrations.AlterField(
            model_name="address",
            name="poBox",
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]
