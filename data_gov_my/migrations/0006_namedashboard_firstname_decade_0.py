# Generated by Django 4.1.7 on 2023-03-15 01:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_gov_my', '0005_remove_namedashboard_firstname_first_name_idx_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='namedashboard_firstname',
            name='decade_0',
            field=models.IntegerField(default=0, null=True),
        ),
    ]
