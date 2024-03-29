# Generated by Django 4.1.7 on 2023-05-09 09:21

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0019_electiondashboard_seats_state"),
    ]

    operations = [
        migrations.AddField(
            model_name="electiondashboard_candidates",
            name="voter_turnout",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="electiondashboard_candidates",
            name="voter_turnout_perc",
            field=models.FloatField(default=0, null=True),
        ),
        migrations.AddField(
            model_name="electiondashboard_candidates",
            name="votes_rejected",
            field=models.IntegerField(default=0),
        ),
        migrations.AddField(
            model_name="electiondashboard_candidates",
            name="votes_rejected_perc",
            field=models.FloatField(default=0, null=True),
        ),
    ]
