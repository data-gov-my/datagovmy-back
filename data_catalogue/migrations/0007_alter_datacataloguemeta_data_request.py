# Generated by Django 4.1.7 on 2023-12-19 11:47

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0002_datarequest_rejection_reason"),
        ("data_catalogue", "0006_datacataloguemeta_data_request"),
    ]

    operations = [
        migrations.AlterField(
            model_name="datacataloguemeta",
            name="data_request",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="published_data",
                to="data_request.datarequest",
            ),
        ),
    ]
