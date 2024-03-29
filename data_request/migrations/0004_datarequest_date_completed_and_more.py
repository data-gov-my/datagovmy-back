# Generated by Django 4.1.7 on 2023-12-20 06:30

from django.db import migrations, models
import django.utils.timezone


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0003_rename_rejection_reason_datarequest_remark_and_more"),
    ]

    operations = [
        migrations.AddField(
            model_name="datarequest",
            name="date_completed",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="datarequest",
            name="date_submitted",
            field=models.DateTimeField(
                auto_now_add=True, default=django.utils.timezone.now
            ),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name="datarequest",
            name="date_under_review",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name="datarequest",
            name="status",
            field=models.CharField(
                choices=[
                    ("submitted", "Submitted"),
                    ("under_review", "Under Review"),
                    ("rejected", "Rejected"),
                    ("data_published", "Data Published"),
                ],
                default="submitted",
                max_length=20,
            ),
        ),
    ]
