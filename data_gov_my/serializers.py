from rest_framework import serializers

from data_gov_my.models import (
    CatalogJson,
    DashboardJson,
    ElectionDashboard_Candidates,
    ElectionDashboard_Party,
    ElectionDashboard_Seats,
    FormData,
    MetaJson,
    i18nJson,
)


class MetaSerializer(serializers.ModelSerializer):
    class Meta:
        model = MetaJson
        fields = ["dashboard_name", "dashboard_meta"]


class DashboardSerializer(serializers.ModelSerializer):
    class Dashboard:
        model = DashboardJson
        fields = ["dashboard_name", "chart_name", "chart_type", "chart_data"]


class CatalogSerializer(serializers.ModelSerializer):
    class Catalog:
        model = CatalogJson
        fields = [
            "id",
            "catalog_meta",
            "catalog_name",
            "catalog_category",
            "time_range",
            "geography",
            "dataset_range",
            "data_source",
            "catalog_data",
        ]


class ElectionCandidateSerializer(serializers.ModelSerializer):
    votes = serializers.SerializerMethodField()

    class Meta:
        model = ElectionDashboard_Candidates
        fields = [
            "name",
            "type",
            "date",
            "election_name",
            "seat",
            "party",
            "votes",
            "result",
            "voter_turnout",
            "voter_turnout_perc",
            "votes_rejected",
            "votes_rejected_perc",
            "majority",
            "majority_perc",
        ]

    def get_votes(self, obj):
        return {"abs": obj.votes, "perc": obj.votes_perc}


class ElectionSeatSerializer(serializers.ModelSerializer):
    majority = serializers.SerializerMethodField()

    class Meta:
        model = ElectionDashboard_Seats
        fields = ["seat", "election_name", "date", "party", "name", "majority", "type"]

    def get_majority(self, obj):
        return {"abs": obj.majority, "perc": obj.majority_perc}


class ElectionPartySerializer(serializers.ModelSerializer):
    votes = serializers.SerializerMethodField()
    seats = serializers.SerializerMethodField()

    class Meta:
        model = ElectionDashboard_Party
        fields = ["party", "type", "state", "election_name", "date", "seats", "votes"]

    def get_votes(self, obj):
        return {"abs": obj.votes, "perc": obj.votes_perc}

    def get_seats(self, obj):
        return {"total": obj.seats_total, "perc": obj.seats_perc, "won": obj.seats}


class ElectionOverallSeatSerializer(serializers.ModelSerializer):
    majority = serializers.SerializerMethodField()
    voter_turnout = serializers.SerializerMethodField()
    votes_rejected = serializers.SerializerMethodField()

    class Meta:
        model = ElectionDashboard_Seats
        fields = [
            "seat",
            "date",
            "party",
            "name",
            "majority",
            "voter_turnout",
            "votes_rejected",
        ]

    def get_majority(self, obj):
        return {"abs": obj.majority, "perc": obj.majority_perc}

    def get_voter_turnout(self, obj):
        return {"abs": obj.voter_turnout, "perc": obj.voter_turnout_perc}

    def get_votes_rejected(self, obj):
        return {"abs": obj.votes_rejected, "perc": obj.votes_rejected_perc}


class i18nSerializer(serializers.ModelSerializer):
    class Meta:
        model = i18nJson
        exclude = ["id"]

    def update(self, instance, validated_data):
        i18n_object = i18nJson.objects.filter(
            filename=instance.filename, language=instance.language
        ).update(**validated_data)
        return i18n_object


class FormDataSerializer(serializers.ModelSerializer):
    class Meta:
        model = FormData
        fields = ["language", "form_data"]
