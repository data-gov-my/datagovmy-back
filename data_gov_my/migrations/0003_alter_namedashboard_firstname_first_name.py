# Generated by Django 4.1.7 on 2023-03-14 06:32

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0002_namedashboard_firstname"),
    ]

    operations = [
        migrations.AlterField(
            model_name="namedashboard_firstname",
            name="first_name",
            field=models.CharField(db_index=True, max_length=300),
        ),
    ]
