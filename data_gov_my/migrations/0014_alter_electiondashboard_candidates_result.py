# Generated by Django 4.1.7 on 2023-04-20 10:31

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0013_alter_electiondashboard_candidates_votes"),
    ]

    operations = [
        migrations.AlterField(
            model_name="electiondashboard_candidates",
            name="result",
            field=models.CharField(max_length=100),
        ),
    ]
