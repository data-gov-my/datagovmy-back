# Generated by Django 4.1.7 on 2023-05-31 18:08

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0028_formdata_formtemplate_delete_modsdata_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="metajson",
            name="route",
            field=models.CharField(max_length=100, null=True),
        ),
    ]
