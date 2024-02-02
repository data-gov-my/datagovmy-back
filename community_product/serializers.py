from rest_framework import serializers
from .models import CommunityProduct


class CommunityProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = CommunityProduct
        fields = "__all__"
