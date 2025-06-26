"""Django admin display."""

from django.contrib import admin

from .models import (
    AttributeMap,
    CategoryMap,
    FeedCategory,
    FeedCategoryAttribute,
    FeedCategoryAttributeValue,
    FeedMeta,
    MarketCategory,
    MarketAttribute,
    MarketAttributeValue,
    MarketAttributeValueDictionary,
    MarketCategoryAttribute,
    Marketplace,
    MapperAlertEmail,
    ValueMap,
    FeedMarketplaceSettings,
    ValueUnit,
    ValueUnitMap,
)


class MapperAlertEmailInline(admin.StackedInline):
    """Inline model for mapper alert email."""

    model = MapperAlertEmail
    extra = 0


@admin.register(CategoryMap)
class CategoryMapAdmin(admin.ModelAdmin):
    """Display for CategoryMap model."""

    autocomplete_fields = ['marketplace_category']

    list_display = [
        'id',
        'feed_category',
        'marketplace_category',
    ]

    search_fields = [
        'feed_category__name',
        'marketplace_category__name',
    ]


@admin.register(AttributeMap)
class AttributeMapAdmin(admin.ModelAdmin):
    """Display for AttributeMap model."""

    list_display = [
        'id',
        'category_map',
        'feed_attribute',
        'marketplace_attribute',
    ]


@admin.register(ValueMap)
class ValueMapAdmin(admin.ModelAdmin):
    """Display for ValueMap model."""

    list_display = [
        'id',
        'attribute_map',
        'feed_attribute_value',
        'marketplace_attribute_value',
    ]


@admin.register(Marketplace)
class MarketplaceAdmin(admin.ModelAdmin):
    """Display for Marketplace model."""

    readonly_fields = (
        'client',
    )

    list_display = [
        'id',
        'marketplace',
    ]

    list_filter = (
        'marketplace',
    )


@admin.register(FeedMeta)
class FeedMetaAdmin(admin.ModelAdmin):
    """Display for FeedMeta model."""

    readonly_fields = (
        'parsed',
        'updated',
        'deleted',
    )

    list_display = [
        'id',
        'url',
        'domain',
        'custom_name',
        'parsed',
    ]

    search_fields = [
        'id',
        'url',
        'domain',
        'custom_name',
    ]

    inlines = [MapperAlertEmailInline]


@admin.register(FeedCategory)
class FeedCategoryAdmin(admin.ModelAdmin):
    """Display for FeedCategory model."""

    list_display = [
        'id',
        'feed',
        'parent',
        'name',
        'is_mapped',
        'deleted',
    ]

    search_fields = [
        'id',
        'feed__domain',
        'name',
    ]

    list_filter = (
        'feed',
    )


@admin.register(FeedCategoryAttribute)
class FeedCategoryAttributeAdmin(admin.ModelAdmin):
    """Display for FeedCategoryAttribute model."""

    list_display = [
        'id',
        'category',
        'name',
        'unit',
        'deleted',
    ]

    search_fields = [
        'name',
        'category__name',
    ]


@admin.register(FeedCategoryAttributeValue)
class FeedCategoryAttributeValueAdmin(admin.ModelAdmin):
    """Display for FeedCategoryAttributeValue model."""

    list_display = [
        'id',
        'attribute',
        'value',
        'deleted',
    ]

    search_fields = [
        'id',
        'value',
    ]


@admin.register(MarketCategory)
class MarketCategoryAdmin(admin.ModelAdmin):
    """Display for MarketCategory model."""

    list_display = [
        'id',
        'parent',
        'source_id',
        'name',
        'marketplace',
        'deleted',
        'leaf',
    ]

    search_fields = [
        'id',
        'name',
    ]

    list_filter = (
        'marketplace',
    )


@admin.register(MarketAttribute)
class MarketAttributeAdmin(admin.ModelAdmin):
    """Display for MarketAttribute model."""

    list_display = [
        'id',
        'dictionary',
        'source_id',
        'name',
        'description',
        'data_type',
        # 'required',
        'deleted',
        'updated',
        'disabled',
        'map_equal_values',
        'default_value_id',
        'map_feed_attribute_name',
        'is_rich_content',
        'ignore_data_type',
    ]

    search_fields = [
        'id',
        'name',
        'source_id',
    ]


@admin.register(MarketAttributeValue)
class MarketAttributeValueAdmin(admin.ModelAdmin):
    """Display for MarketAttributeValue model."""

    readonly_fields = [
        'id',
        'dictionary',
        'source_id',
        'value',
        'deleted',
    ]

    list_display = [
        'value',
        'source_id',
        'deleted',
    ]

    search_fields = [
        'id',
        'dictionary__source_id',
        'source_id',
        'value',
    ]


@admin.register(FeedMarketplaceSettings)
class FeedMarketplaceSettingsAdmin(admin.ModelAdmin):
    """Display for FeedMarketplaceSettings model."""

    list_display = [
        'id',
        'feed',
        'marketplace',
        'content_type',
        'object_id',
        'hidden',
    ]

    search_fields = [
        'id',
        'feed__domain',
        'marketplace__marketplace',
        'content_type__model',
        'object_id',
    ]


@admin.register(MarketAttributeValueDictionary)
class MarketAttributeValueDictionaryAdmin(admin.ModelAdmin):
    """Display for MarketAttributeValueDictionary model."""

    list_display = [
        'id',
        'source_id',
    ]

    search_fields = [
        'id',
        'source_id',
    ]


@admin.register(MarketCategoryAttribute)
class MarketCategoryAttributeAdmin(admin.ModelAdmin):
    """Display for MarketCategoryAttribute model."""

    list_display = [
        'id',
        'category',
        'attribute',
        'required',
        'is_collection',
    ]

    search_fields = [
        'id',
        'category__name',
        'attribute__name',
    ]


@admin.register(ValueUnit)
class ValueUnitAdmin(admin.ModelAdmin):
    """Display for ValueUnit model."""

    list_display = [
        'id',
        'name',
    ]

    search_fields = [
        'id',
        'name',
    ]


@admin.register(ValueUnitMap)
class ValueUnitMapAdmin(admin.ModelAdmin):
    """Display for ValueUnitMap model."""

    list_display = [
        'id',
        'value_unit_from',
        'value_unit_to',
        'multiplier',
    ]

    search_fields = [
        'id',
        'value_unit_from__name',
        'value_unit_to__name',
    ]
