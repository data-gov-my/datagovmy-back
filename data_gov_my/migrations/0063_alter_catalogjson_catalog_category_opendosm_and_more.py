# Generated by Django 4.1.7 on 2023-09-08 09:04

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0062_catalogjson_catalog_category_opendosm_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="catalogjson",
            name="catalog_category_opendosm",
            field=models.CharField(max_length=300, null=True),
        ),
        migrations.AlterField(
            model_name="catalogjson",
            name="catalog_category_opendosm_name",
            field=models.CharField(max_length=600, null=True),
        ),
        migrations.AlterField(
            model_name="catalogjson",
            name="catalog_subcategory_opendosm",
            field=models.CharField(max_length=300, null=True),
        ),
        migrations.AlterField(
            model_name="catalogjson",
            name="catalog_subcategory_opendosm_name",
            field=models.CharField(max_length=600, null=True),
        ),
    ]