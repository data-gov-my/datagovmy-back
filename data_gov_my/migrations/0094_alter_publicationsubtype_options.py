# Generated by Django 4.1.7 on 2024-11-17 09:41

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        (
            "data_gov_my",
            "0093_alter_publicationsubtype_id_alter_publicationtype_id_and_more",
        ),
    ]

    operations = [
        migrations.AlterModelOptions(
            name="publicationsubtype",
            options={"ordering": ["publication_type__order", "order"]},
        ),
    ]