# Generated by Django 4.1.7 on 2023-05-03 01:39

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0017_electiondashboard_party_and_more"),
    ]

    operations = [
        migrations.RenameField(
            model_name="i18njson",
            old_name="translation_json",
            new_name="translation",
        ),
        migrations.AlterField(
            model_name="i18njson",
            name="route",
            field=models.CharField(max_length=100),
        ),
    ]
