# Generated by Django 4.1.7 on 2023-12-20 04:29

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0002_datarequest_rejection_reason"),
    ]

    operations = [
        migrations.RenameField(
            model_name="datarequest",
            old_name="rejection_reason",
            new_name="remark",
        ),
        migrations.AddField(
            model_name="datarequest",
            name="remark_en",
            field=models.TextField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="datarequest",
            name="remark_ms",
            field=models.TextField(blank=True, null=True),
        ),
    ]