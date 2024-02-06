from rest_framework import serializers
from .models import CommunityProduct


class CommunityProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = CommunityProduct
        fields = [
            "id",
            "name",
            "email",
            "institution",
            "product_name",
            "product_description",
            "problem_statement",
            "solutions_developed",
            "product_type",
            "product_year",
            "product_link",
            "dataset_used",
            "status",
            "created_at",
            "language",
            "date_approved",
        ]
