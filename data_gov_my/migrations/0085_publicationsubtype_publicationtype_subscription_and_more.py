# Generated by Django 4.1.7 on 2024-10-29 18:31

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0084_car"),
    ]

    operations = [
        migrations.CreateModel(
            name="PublicationSubtype",
            fields=[
                (
                    "id",
                    models.CharField(max_length=50, primary_key=True, serialize=False),
                ),
                ("subtype_en", models.CharField(max_length=50)),
                ("subtype_bm", models.CharField(max_length=50)),
            ],
        ),
        migrations.CreateModel(
            name="PublicationType",
            fields=[
                (
                    "id",
                    models.CharField(max_length=50, primary_key=True, serialize=False),
                ),
                ("type_en", models.CharField(max_length=50)),
                ("type_bm", models.CharField(max_length=50)),
            ],
        ),
        migrations.CreateModel(
            name="Subscription",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                (
                    "language",
                    models.CharField(
                        choices=[("en", "English"), ("ms", "Bahasa Melayu")],
                        default="en",
                        max_length=2,
                    ),
                ),
                ("email", models.EmailField(max_length=254, unique=True)),
                (
                    "publications",
                    models.ManyToManyField(to="data_gov_my.publicationsubtype"),
                ),
            ],
        ),
        migrations.AddField(
            model_name="publicationsubtype",
            name="publication_type",
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.CASCADE,
                to="data_gov_my.publicationtype",
            ),
        ),
    ]
