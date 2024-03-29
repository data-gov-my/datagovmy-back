# Generated by Django 4.1.7 on 2023-08-20 02:53

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0048_publicationupcoming_publication_type_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="publication",
            name="publication_type_title",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publication",
            name="title",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationdocumentation",
            name="publication_type",
            field=models.CharField(max_length=100),
        ),
        migrations.AlterField(
            model_name="publicationdocumentation",
            name="publication_type_title",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationdocumentation",
            name="title",
            field=models.CharField(max_length=100),
        ),
        migrations.AlterField(
            model_name="publicationdocumentationresource",
            name="resource_name",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationdocumentationresource",
            name="resource_type",
            field=models.CharField(max_length=100),
        ),
        migrations.AlterField(
            model_name="publicationupcoming",
            name="product_type",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationupcoming",
            name="publication_title",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationupcoming",
            name="publication_type_title",
            field=models.CharField(max_length=150),
        ),
        migrations.AlterField(
            model_name="publicationupcoming",
            name="release_series",
            field=models.CharField(max_length=150),
        ),
    ]
