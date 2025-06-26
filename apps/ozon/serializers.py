"""Serializers for Ozon app."""

from rest_framework import serializers
from .models import (
    OzonAuthKey,
    OzonFeedUrl,
)


class AuthKeySerializer(serializers.ModelSerializer):
    """Serializer for OzonAuthKey model."""

    class Meta:
        """Set model info."""

        model = OzonAuthKey
        fields = [
            'domain',
            'client_id',
            'api_key',
            'is_disabled',
            'enable_posting',
            'full_update_allowed',
        ]


class FeedUrlSerializer(serializers.ModelSerializer):
    """Serializer for OzonFeedUrl model."""

    class Meta:
        """Set model info."""

        model = OzonFeedUrl
        fields = [
            'domain',
            'url',
            'parsed',
            'updated',
            'deleted',
            'collection_name',
            'error',
            'custom_name',
            'login',
            'password',
            'timestamp_modified',
            'last_parsed_date',
        ]



class AuthDataSerializer(serializers.Serializer):
    """Serializer for Ozon auth data."""

    client_id = serializers.CharField()
    api_key = serializers.CharField()
