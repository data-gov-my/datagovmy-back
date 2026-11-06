from django.db import migrations

# The unused `address` app was removed. Its own migrations no longer run, so drop its
# table and the bookkeeping rows Django kept for it. One-way: the app is gone.
ADDRESS_CONTENT_TYPES = "SELECT id FROM django_content_type WHERE app_label = 'address'"
ADDRESS_PERMISSIONS = f"SELECT id FROM auth_permission WHERE content_type_id IN ({ADDRESS_CONTENT_TYPES})"

SQL = [
    "DROP TABLE IF EXISTS address_address CASCADE",
    f"DELETE FROM auth_group_permissions WHERE permission_id IN ({ADDRESS_PERMISSIONS})",
    f"DELETE FROM auth_user_user_permissions WHERE permission_id IN ({ADDRESS_PERMISSIONS})",
    f"DELETE FROM auth_permission WHERE content_type_id IN ({ADDRESS_CONTENT_TYPES})",
    f"UPDATE django_admin_log SET content_type_id = NULL WHERE content_type_id IN ({ADDRESS_CONTENT_TYPES})",
    "DELETE FROM django_content_type WHERE app_label = 'address'",
    "DELETE FROM django_migrations WHERE app = 'address'",
]


class Migration(migrations.Migration):
    dependencies = [
        ("data_gov_my", "0096_publication_description_email"),
        ("auth", "0012_alter_user_first_name_max_length"),
        ("contenttypes", "0002_remove_content_type_name"),
        ("admin", "0003_logentry_add_action_flag_choices"),
    ]

    operations = [migrations.RunSQL(sql, reverse_sql=migrations.RunSQL.noop) for sql in SQL]
