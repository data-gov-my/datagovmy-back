# Generated by Django 4.1.7 on 2023-12-13 07:46

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0073_alter_explorersupdate_data_next_update"),
    ]

    operations = [
        migrations.CreateModel(
            name="ExplorersMetaJson",
            fields=[
                (
                    "metajson_ptr",
                    models.OneToOneField(
                        auto_created=True,
                        on_delete=django.db.models.deletion.CASCADE,
                        parent_link=True,
                        primary_key=True,
                        serialize=False,
                        to="data_gov_my.metajson",
                    ),
                ),
            ],
            bases=("data_gov_my.metajson",),
        ),
    ]