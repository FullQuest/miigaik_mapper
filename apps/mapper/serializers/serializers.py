"""Internal API serializers for mapper app."""

from django.apps import apps

from apps.mapper.models import (
    AttributeMap,
    CategoryMap,
    FeedCategory,
    FeedCategoryAttribute,
    FeedCategoryAttributeValue,
    FeedMeta,
    MarketCategory,
    MarketAttribute,
    MarketAttributeValue,
    MarketCategoryAttribute,
    Marketplace,
    ValueMap,
    FeedMarketplaceSettings,
)

from rest_framework.serializers import ModelSerializer
from rest_framework import serializers


class CategoryMapSerializer(ModelSerializer):
    """Serializer for CategoryMap model."""

    class Meta:
        """Set model info."""

        model = CategoryMap
        fields = '__all__'


class AttributeMapSerializer(ModelSerializer):
    """Serializer for AttributeMap model."""

    class Meta:
        """Set model info."""

        model = AttributeMap
        fields = '__all__'


class ValueMapSerializer(ModelSerializer):
    """Serializer for ValueMap model."""

    class Meta:
        """Set model info."""

        model = ValueMap
        fields = '__all__'


class FeedMetaSerializer(ModelSerializer):
    """Serializer for FeedMeta model."""

    class Meta:
        """Set model info."""

        model = FeedMeta
        fields = [
            'id',
            'url',
            'domain',
            'custom_name',
            'login',
            'password',
            'parsed',
            'updated',
            'deleted',
            'last_parsed_date',
            'error',
            'error_description',
            'sync_data',
        ]


class FeedMetaCustomSerializer(ModelSerializer):
    """Serializer for FeedMeta model."""

    class Meta:
        """Set model info."""

        model = FeedMeta
        fields = (
            'id',
            'url',
            'domain',
            'custom_name',
        )


class FeedCategorySerializer(ModelSerializer):
    """Serializer for FeedCategory model."""

    class Meta:
        """Set model info."""

        model = FeedCategory
        fields = [
            'id',
            'feed',
            'parent',
            'name',
            'deleted',
            'is_mapped',
            'mapping_data',
            'children',
        ]
        read_only_fields = [
            'id',
            'feed',
            'parent',
            'name',
            'deleted',
            'is_mapped',
            'mapping_data',
        ]


class FeedCategoryListSerializer(ModelSerializer):
    """Serializer for FeedCategory model."""

    class Meta:
        """Set model info."""

        model = FeedCategory
        fields = [
            'id',
            'feed',
            'parent',
            'name',
            'deleted',
            'is_mapped',
            'mapping_data',
            'source_id',
        ]
        read_only_fields = [
            'id',
            'feed',
            'parent',
            'name',
            'deleted',
            'is_mapped',
            'mapping_data',
            'source_id',
        ]


class FeedCategoryAttributeSerializer(ModelSerializer):
    """Serializer for FeedCategoryAttribute model."""

    unit = serializers.CharField()

    class Meta:
        """Set model info."""

        model = FeedCategoryAttribute
        fields = [
            'id',
            'category',
            'name',
            'unit',
            'deleted',
            'is_mapped',
            'mapping_data',
        ]


class FeedCategoryAttributeValueSerializer(ModelSerializer):
    """Serializer for FeedCategoryAttributeValue model."""

    class Meta:
        """Set model info."""

        model = FeedCategoryAttributeValue
        fields = [
            'id',
            'attribute',
            'value',
            'deleted',
            'is_mapped',
            'mapping_data',
        ]


class MarketplaceSerializer(ModelSerializer):
    """Serializer for Marketplace view."""

    class Meta:
        """Set model info."""

        model = Marketplace
        fields = '__all__'


class MarketCategorySerializer(ModelSerializer):
    """Serializer for MarketCategory model."""

    class Meta:
        """Set model info."""

        model = MarketCategory
        fields = [
            'id',
            'marketplace',
            'parent',
            'name',
            'deleted',
            'updated',
            'leaf',
        ]


class MarketCategoryAttributeSerializer(ModelSerializer):
    """Serializer for MarketCategoryAttribute model."""

    name = serializers.CharField(source='attribute.name')
    unit = serializers.CharField(source='attribute.unit')
    dictionary_id = serializers.IntegerField(source='attribute.dictionary_id')

    class Meta:
        """Set model info."""

        model = MarketCategoryAttribute
        fields = [
            'id',
            'category',
            'name',
            'unit',
            'required',
            'deleted',
            'is_collection',
            'is_mapped',
            'mapping_data',
            'dictionary_id',
        ]


class MarketAttributeSerializer(ModelSerializer):
    """Serializer for MarketAttribute model."""

    class Meta:
        """Set model info."""

        model = MarketAttribute
        fields = [
            'id',
            'dictionary',
            'name',
            'description',
            'data_type',
            'required',
            # 'deleted',
            'updated',
        ]


class MarketAttributeValueSerializer(ModelSerializer):
    """Serializer for MarketAttributeValue model."""

    class Meta:
        """Set model info."""

        model = MarketAttributeValue
        fields = [
            'id',
            'dictionary',
            'value',
            'info',
            'picture_url',
            'deleted',
            'is_mapped',
            'mapping_data',
        ]


class FeedMarketplaceSettingsSerializer(ModelSerializer):
    """Serializer for FeedMarketplaceSettings model."""

    content_type = serializers.SerializerMethodField()

    class Meta:
        model = FeedMarketplaceSettings
        fields = '__all__'
        read_only_fields = ('content_type', 'object_id')

    def to_internal_value(self, data):
        def _get_item_ret(_data):
            object_id = _data.pop('object_id')
            content_type = _data.pop('content_type')

            ret = super(
                FeedMarketplaceSettingsSerializer,
                self
            ).to_internal_value(_data)

            ret['object_id'] = object_id
            ret['content_type'] = content_type
            return ret

        if isinstance(data, list):
            return map(_get_item_ret, data)

        return _get_item_ret(data)

    def validate(self, data):
        object_id = data.pop('object_id')
        content_type = f'mapper.{data.pop("content_type")}'

        model = apps.get_model(content_type)

        try:
            content_object = model.objects.get(id=object_id)
        except model.DoesNotExist:
            raise serializers.ValidationError('Not found')
        else:
            data['content_object'] = content_object

        return data

    def create(self, validate_data):
        return FeedMarketplaceSettings.objects.create(**validate_data)

    def get_content_type(self, instance):
        return instance.content_object.__class__.__name__

