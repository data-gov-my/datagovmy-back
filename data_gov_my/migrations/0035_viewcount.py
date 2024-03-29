# Generated by Django 4.1.7 on 2023-06-21 04:05

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0034_electiondashboard_seats_voter_turnout_and_more"),
    ]

    operations = [
        migrations.CreateModel(
            name="ViewCount",
            fields=[
                (
                    "id",
                    models.CharField(max_length=100, primary_key=True, serialize=False),
                ),
                ("type", models.CharField(max_length=100)),
                ("all_time_view", models.IntegerField(default=0)),
                ("download_csv", models.IntegerField(default=0)),
                ("download_parquet", models.IntegerField(default=0)),
                ("download_png", models.IntegerField(default=0)),
                ("download_svg", models.IntegerField(default=0)),
            ],
        ),
    ]
