# Generated by Django 4.1.7 on 2023-09-01 10:39

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0055_ktmbtimeseries_ktmbtimeseriescallout"),
    ]

    operations = [
        migrations.AlterField(
            model_name="ktmbtimeseries",
            name="passengers",
            field=models.IntegerField(null=True),
        ),
        migrations.AlterField(
            model_name="ktmbtimeseriescallout",
            name="passengers",
            field=models.IntegerField(null=True),
        ),
    ]
