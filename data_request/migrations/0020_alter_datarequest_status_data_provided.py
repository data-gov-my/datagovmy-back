from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_request", "0019_datarequest_live_remark_en_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="datarequest",
            name="status",
            field=models.CharField(
                choices=[
                    ("submitted", "Submitted"),
                    ("under_review", "Under Review"),
                    ("rejected", "Rejected"),
                    ("data_provided", "Data Provided"),
                    ("data_published", "Data Published"),
                ],
                default="submitted",
                max_length=20,
            ),
        ),
    ]
