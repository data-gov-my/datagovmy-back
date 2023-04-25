# Generated by Django 4.1.7 on 2023-04-25 04:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_gov_my', '0015_alter_electiondashboard_candidates_votes_perc'),
    ]

    operations = [
        migrations.CreateModel(
            name='ElectionDashboard_Seats',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('seat', models.CharField(max_length=100)),
                ('election_name', models.CharField(max_length=100)),
                ('date', models.CharField(max_length=100)),
                ('party', models.CharField(max_length=100)),
                ('name', models.CharField(max_length=100)),
                ('type', models.CharField(max_length=20)),
                ('majority', models.IntegerField()),
                ('majority_perc', models.FloatField(null=True)),
                ('seat_name', models.CharField(max_length=100)),
            ],
        ),
    ]