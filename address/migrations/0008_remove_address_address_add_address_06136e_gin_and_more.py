# Generated by Django 4.1.7 on 2024-01-19 07:20

import django.contrib.postgres.indexes
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("address", "0007_address_custom_btree_migration"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="address",
            name="address_add_address_06136e_gin",
        ),
        migrations.AddIndex(
            model_name="address",
            index=django.contrib.postgres.indexes.GinIndex(
                fields=["address"], name="address_gin_idx", opclasses=["gin_trgm_ops"]
            ),
        ),
    ]
