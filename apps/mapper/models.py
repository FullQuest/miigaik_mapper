"""Models for mapper app."""

from typing import Any, Dict, Iterable, List, Optional

from django.contrib.contenttypes.fields import GenericForeignKey
from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import ValidationError
from django.db import models

MappingData = Optional[Dict[str, Any]]
MappingDataStorage = Optional[List[Dict[str, Any]]]
SyncData = Optional[List[Dict[str, Any]]]


class FeedMeta(models.Model):
    """FeedMeta model for mapper.

    :param str url: Feed url
    :param str domain: Domain name
    :param str custom_name: Custom name for feed
    :param str login: Login for feed download
    :param str password: Pass for feed download

    :param bool parsed: Parsed state
    :param bool updated: Updated state
    :param bool deleted: Deleted state

    :param datetime last_parsed_date: Last parsed date
    :param str error: Parsing error
    :param str error_description: Parsing error description
    """

    url = models.TextField()

    domain = models.CharField(max_length=50, unique=True)
    custom_name = models.CharField(max_length=50, blank=True, null=True)
    login = models.CharField(max_length=50, blank=True, null=True)
    password = models.CharField(max_length=50, blank=True, null=True)

    parsed = models.SmallIntegerField(default=0)
    updated = models.SmallIntegerField(default=0)
    deleted = models.BooleanField(default=False)

    last_parsed_date = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Last date of successful parse feed',
    )
    parsed_hash = models.TextField(blank=True, null=True, editable=False)
    error = models.CharField(max_length=50, blank=True, null=True)
    error_description = models.TextField(blank=True)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Feeds"

    def __str__(self) -> str:
        """Return string representation of FeedMeta model."""
        return f"{self.domain} [{self.id}]"

    @property
    def sync_data(self) -> SyncData:
        """Return marketplaces' sync data."""
        return [
            {
                'marketplace': _map.marketplace.marketplace,
                'last_sync_date': _map.last_sync_date,
                'error': _map.error,
            }
            for _map in
            FeedMarketplaceMap.objects.filter(
                feed=self,
            )
        ]

    @property
    def alert_emails_list(self) -> Iterable[str]:
        """Get emails list of feed for sending base messages."""
        return self.get_alert_emails_list(
            filters={
                'send_base_alerts': True,
            },
        )

    def get_alert_emails_list(self, filters) -> Iterable[str]:
        """Get emails list of feed for sending messages."""
        return (
            # NOTE: mypy and pylance can't compute related django orm fields
            self.alert_emails  # type: ignore
            .filter(**filters)
            .values_list('email', flat=True)
        )


class MapperAlertEmail(models.Model):
    """MapperAlertEmail class."""

    feed = models.ForeignKey(
        'FeedMeta',
        models.CASCADE,
        related_name='alert_emails',
    )
    email = models.EmailField()
    send_notifications = models.BooleanField(
        default=False, verbose_name='Подписаться на задачи по маппингу')
    notifications_time = models.TimeField(
        null=True,
        blank=True,
        help_text='Время для рассылки задач по маппингу',
    )
    get_mapping_report = models.BooleanField(
        default=False,
        verbose_name='Получать отчет по маппингу',
    )

    def __str__(self):
        """Return string representation of MapperAlertEmail object."""
        return self.email

    class Meta:
        """Unique together constraints."""

        unique_together = ['feed', 'email']


class FeedCategory(models.Model):
    """FeedCategory model for mapper.

    :param int feed: Foreign key to FeedMeta
    :param int parent: Foreign key to self
    :param int source_id: Source category id from feed

    :param str name: Category name

    :param bool deleted: Deleted state
    """

    feed = models.ForeignKey(
        'FeedMeta',
        on_delete=models.CASCADE,
    )
    parent = models.ForeignKey(
        'self',
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    source_id = models.BigIntegerField()

    name = models.CharField(max_length=255)

    deleted = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Feed categories"

    def __str__(self) -> str:
        """Return string representation of FeedCategory model."""
        return f"{self.name} [{self.id}]"

    @property
    def is_mapped(self) -> bool:
        """Check if category is mapped."""
        if CategoryMap.objects.filter(
            feed_category=self.id,
        ).exists():
            return True

        return False

    @property
    def mapping_data(self) -> MappingDataStorage:
        """Return mapping related data."""
        try:
            mappings = CategoryMap.objects.filter(
                feed_category=self.id,
            )

            mapping_data = [
                mapping.get_mapping_data()
                for mapping
                in mappings
            ]

        except CategoryMap.DoesNotExist:
            return None

        return mapping_data

    @property
    def children(self) -> List[Dict[str, Any]]:
        """Show category children."""
        children = FeedCategory.objects.filter(parent=self.id)

        data = [
            {
                'id': category.id,
                'feed_id': category.feed.id,
                'parent_id': category.parent.id,
                'name': category.name,
                'deleted': category.deleted,
                'is_mapped': category.is_mapped,
                'mapping_data': category.mapping_data,
                'children': category.children,
            } for category in children
        ]

        return data

    def get_parents(self):
        """Get parent categories."""
        if self.parent is None:
            return FeedCategory.objects.none()

        return FeedCategory.objects.filter(
            pk=self.parent.pk) | self.parent.get_parents()


class FeedCategoryAttribute(models.Model):
    """FeedCategoryAttribute model for mapper.

    :param int category: Foreign key to FeedCategory

    :param str name: Attribute name
    :param str unit: Attribute unit

    :param bool deleted: Deleted state
    :param bool is_tag: Attribute is tag
    """

    category = models.ForeignKey(
        'FeedCategory',
        on_delete=models.CASCADE,
    )

    name = models.CharField(max_length=255)
    unit = models.ForeignKey(
        'ValueUnit',
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    deleted = models.BooleanField(default=False)

    is_tag = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Feed category attributes"

    def __str__(self) -> str:
        """Return string representation of FeedCategoryAttribute model."""
        return f"{self.name} [{self.id}]"

    @property
    def is_mapped(self) -> bool:
        """Check if attribute is mapped."""
        if AttributeMap.objects.filter(
            feed_attribute=self.id,
        ).exists():
            return True

        return False

    @property
    def mapping_data(self) -> MappingDataStorage:
        """Return mapping related data."""
        try:
            mappings = AttributeMap.objects.filter(
                feed_attribute=self.id,
            )

            mapping_data = [
                mapping.get_mapping_data()
                for mapping
                in mappings
            ]

        except AttributeMap.DoesNotExist:
            return None

        return mapping_data


class FeedCategoryAttributeValue(models.Model):
    """FeedCategoryAttributeValue model for mapper.

    :param int attribute: Foreign key to FeedCategoryAttribute

    :param str value: Attribute value

    :param bool deleted: Deleted state
    """

    attribute = models.ForeignKey(
        'FeedCategoryAttribute',
        on_delete=models.CASCADE,
    )

    value = models.TextField(blank=False, null=False)

    deleted = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Feed category attribute values"

    def __str__(self) -> str:
        """Return string representation of FeedCategoryAttributeValue model."""
        return f"{self.value}"

    @property
    def is_mapped(self) -> bool:
        """Check if value is mapped."""
        if ValueMap.objects.filter(
            feed_attribute_value=self.id,
        ).exists():
            return True

        return False

    @property
    def mapping_data(self) -> MappingDataStorage:
        """Return mapping related data."""
        try:
            mappings = ValueMap.objects.filter(
                feed_attribute_value=self.id,
            )

            mapping_data = [
                mapping.get_mapping_data()
                for mapping
                in mappings
            ]

        except ValueMap.DoesNotExist:
            return None

        return mapping_data


class Marketplace(models.Model):
    """Marketplace model for mapper.

    :param str marketplace: Marketplace name
    :param str client: B2basket by default
    :param str api_key: API key
    :param str api_url: API url
    :param str sandbox_url: Sandbox url

    :param int client_id: Client id in marketplace system
    """

    BERU = 'beru'
    LAMODA = 'lamoda'
    OZON = 'ozon'
    WILDBERRIES = 'wildberries'
    YANDEX = 'yandex'

    MARKETPLACES = [
        (BERU, 'Beru'),
        (LAMODA, 'Lamoda'),
        (OZON, 'Ozon'),
        (WILDBERRIES, 'Wildberries'),
        (YANDEX, 'Yandex'),
    ]

    marketplace = models.CharField(
        max_length=50,
        choices=MARKETPLACES,
        unique=True,
    )
    client = models.CharField(default="B2basket", max_length=10)
    api_key = models.CharField(max_length=100)
    api_url = models.CharField(max_length=100, blank=True, null=True)
    sandbox_url = models.CharField(max_length=100, blank=True, null=True)

    client_id = models.IntegerField(blank=False, null=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplaces"

    def __str__(self) -> str:
        """Return string representation of Marketplace model."""
        return f"{self.marketplace}"


class MarketCategory(models.Model):
    """MarketCategory model for mapper.

    :param str marketplace: Foreign key to Marketplace
    :param str name: Category name

    :param int parent: Foreign key to self
    :param int source_id: Original id from marketplace

    :param bool deleted: Deleted state
    :param bool updated: Updated state
    :param bool leaf: Leaf node
    """

    marketplace = models.ForeignKey(
        'Marketplace',
        on_delete=models.CASCADE,
    )
    parent = models.ForeignKey(
        'self',
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    name = models.CharField(max_length=255)

    source_id = models.CharField(max_length=255, blank=True, null=True)

    deleted = models.BooleanField(default=False)
    updated = models.BooleanField(default=False)
    leaf = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplace categories"

    def __str__(self) -> str:
        """Return string representation of MarketCategory model."""
        return f"{self.name}"


class MarketCategoryAttribute(models.Model):
    """MarketCategoryAttribute model for mapper.

    :param int category: Foreign key to marketplace category
    :param int attribute: Foreign key to marketplace attribute

    :param bool restricted: True if attribute is restricted
    :param bool required: True if attribute is required
    :param bool is_collection: True if attribute is collection
    :param int max_count: Max collection values
    """

    category = models.ForeignKey(
        'MarketCategory',
        on_delete=models.CASCADE,
    )

    attribute = models.ForeignKey(
        'MarketAttribute',
        on_delete=models.CASCADE,
    )

    required = models.BooleanField(default=False)
    restricted = models.BooleanField(default=False)

    is_collection = models.BooleanField(default=False)

    max_count = models.IntegerField(blank=True, null=True)
    deleted = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplace category attributes"

    def __str__(self) -> str:
        """Return string representation of MarketCategoryAttribute model."""
        return f"{self.attribute}"

    @property
    def is_mapped(self) -> bool:
        """Check if category is mapped."""
        if AttributeMap.objects.filter(
            marketplace_attribute=self.id,
        ).exists():
            return True

        return False

    @property
    def mapping_data(self) -> MappingDataStorage:
        """Return mapping related data."""
        try:
            mappings = AttributeMap.objects.filter(
                marketplace_attribute=self.id,
            )

            mapping_data = [
                mapping.get_mapping_data()
                for mapping
                in mappings
            ]

        except AttributeMap.DoesNotExist:
            return None

        return mapping_data


class MarketAttribute(models.Model):
    """Market attribute model for mapper.

    :param int dictionary: Foreign key to MarketAttributeValueDictionary
    :param int source_id: Original id from marketplace

    :param str name: Market attribute name
    :param str description: Market attribute description
    :param str data_type: Market attribute data type

    :param bool restricted: True if attribute is restricted
    :param bool required: True if attribute is required
    :param bool deleted: Soft deleted state
    :param bool updated: Updated state
    :param bool disabled: True if attribute is disabled
    :param bool is_rich_content: True if rich content
    """

    SPECIAL_ATTRIBUTE_IDS = {
        'type': '8229'
    }

    dictionary = models.ForeignKey(
        'MarketAttributeValueDictionary',
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    source_id = models.CharField(max_length=255, blank=True, null=True)

    name = models.TextField(max_length=255)
    description = models.TextField(blank=True, null=True)
    data_type = models.TextField(blank=True, null=True)
    unit = models.ForeignKey(
        'ValueUnit',
        on_delete=models.PROTECT,
        blank=True,
        null=True,
    )

    restricted = models.BooleanField(default=False)
    required = models.BooleanField(default=False)
    deleted = models.BooleanField(default=False)
    updated = models.BooleanField(default=False)
    disabled = models.BooleanField(default=False)
    ignore_data_type = models.BooleanField(default=False)

    map_equal_values = models.BooleanField(default=False)
    default_value_id = models.IntegerField(blank=True, null=True)
    map_feed_attribute_name = models.CharField(
        max_length=255,
        blank=True,
        null=True,
    )
    is_rich_content = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplace attributes"
        indexes = [
            models.Index(fields=['source_id']),
        ]

    def __str__(self) -> str:
        """Return string repr of MarketAttributeValueDictionary model."""
        return f"{self.name}"


class MarketAttributeValueDictionary(models.Model):
    """Market attribute value dictionary model.

    :param int source_id: Original dictionary id from marketplace
    :param str name: Dictionary name from marketplace
    """

    source_id = models.CharField(max_length=255, blank=True, null=True)
    name = models.CharField(max_length=255, blank=True, null=True)
    deleted = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplace attribute value dictionaries"
        indexes = [
            models.Index(fields=['source_id']),
        ]

    def __str__(self) -> str:
        """Return string repr of MarketAttributeValueDictionary model."""
        return f"{self.source_id or self.name}"


class MarketAttributeValue(models.Model):
    """Market attribute value model.

    :param int dictionary: Foreign key to MarketAttributeValueDictionary
    :param int source_id: Original id from marketplace
    :param str value: Attribute value

    :param bool deleted: Delete state
    """

    dictionary = models.ForeignKey(
        'MarketAttributeValueDictionary',
        on_delete=models.CASCADE,
    )

    source_id = models.CharField(max_length=255, blank=True, null=True)

    value = models.TextField(blank=False, null=False)

    deleted = models.BooleanField(default=False)

    info = models.TextField(blank=True, null=True)

    picture_url = models.TextField(max_length=300, blank=True, null=True)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Marketplace category attribute values"
        indexes = [
            models.Index(fields=['source_id']),
        ]

    def __str__(self) -> str:
        """Return string repr of MarketAttributeValue model."""
        return f"{self.value}"

    # FIXME
    @property
    def is_mapped(self) -> bool:
        """Check if category attribute value is mapped."""
        if ValueMap.objects.filter(
            marketplace_attribute_value=self.id,
        ).exists():
            return True

        return False

    @property
    def mapping_data(self) -> MappingDataStorage:
        """Return mapping related data."""
        try:
            mappings = ValueMap.objects.filter(
                marketplace_attribute_value=self.id,
            )

            mapping_data = [
                mapping.get_mapping_data()
                for mapping
                in mappings
            ]

        except ValueMap.DoesNotExist:
            return None

        return mapping_data


class CategoryMap(models.Model):
    """CategoryMap model for mapper.

    :param int feed_category: Foreign key to FeedCategory
    :param int marketplace_category: Foreign key to MarketCategory
    """

    feed_category = models.ForeignKey(
        'FeedCategory',
        on_delete=models.CASCADE,
    )

    marketplace_category = models.ForeignKey(
        'MarketCategory',
        on_delete=models.CASCADE,
    )

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Category mappings"
        unique_together = ['feed_category', 'marketplace_category']

    def __str__(self) -> str:
        """Return string representation of CategoryMap model."""
        return f"{self.feed_category} : {self.marketplace_category}"

    def get_mapping_data(self) -> MappingData:
        """Get dict with mapping data."""
        mapping_data = {
            'mapping_id': self.id,
            'feed_id': self.feed_category.feed.id,
            'feed_category_id': self.feed_category.id,
            'feed_category': self.feed_category.name,
            'marketplace_id': self.marketplace_category.marketplace.id,
            'marketplace_category_id': self.marketplace_category.id,
            'marketplace_category': self.marketplace_category.name,
            'marketplace_category_deleted': self.marketplace_category.deleted,
        }

        return mapping_data


class AttributeMap(models.Model):
    """AttributeMap model for mapper.

    :param int feed_category_map: Foreign key to CategoryMap
    :param int feed_attribute: Foreign key to FeedCategoryAttribute
    :param int marketplace_attribute: Foreign key to MarketCategoryAttribute
    """

    def save(self, *args, **kwargs):
        """Save method for AttributeMap model."""
        if (
            self.category_map.feed_category_id !=
            self.feed_attribute.category_id
        ):
            raise ValidationError("Category map conflict")
        super().save(*args, **kwargs)

    category_map = models.ForeignKey(
        'CategoryMap',
        on_delete=models.CASCADE,
    )

    feed_attribute = models.ForeignKey(
        'FeedCategoryAttribute',
        on_delete=models.CASCADE,
    )

    marketplace_attribute = models.ForeignKey(
        'MarketCategoryAttribute',
        on_delete=models.CASCADE,
    )

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Attribute mappings"
        unique_together = [
            'category_map',
            'feed_attribute',
            'marketplace_attribute',
        ]

    def __str__(self) -> str:
        """Return string representation of AttributeMap model."""
        return f"{self.feed_attribute} : {self.marketplace_attribute}"

    def get_mapping_data(self) -> MappingData:
        """Get dict with mapping data."""
        mapping_data = {
            'mapping_id': self.id,
            'feed_id': self.feed_attribute.category.feed.id,
            'feed_category_id': self.feed_attribute.category.id,
            'feed_attribute_id': self.feed_attribute.id,
            'feed_attribute': self.feed_attribute.name,
            'feed_attribute_unit': getattr(
                self.feed_attribute.unit,
                'name',
                None,
            ),
            'marketplace_id':
            self.marketplace_attribute.category.marketplace.id,
            'marketplace_attribute_id': self.marketplace_attribute.id,
            'marketplace_attribute': self.marketplace_attribute.attribute.name,
        }

        return mapping_data


class ValueMap(models.Model):
    """AttributeMap model for mapper.

    :param int feed_category_map: CategoryMap id
    :param int info_model_attribute: Info model attribute
    :param int info_model_attr_value: Info model atttribute value
    :param int feed_attribute: Feed attribute id
    :param int feed_attr_value: Feed attribute value id
    """

    attribute_map = models.ForeignKey(
        'AttributeMap',
        on_delete=models.CASCADE,
    )

    feed_attribute_value = models.ForeignKey(
        'FeedCategoryAttributeValue',
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    marketplace_attribute_value = models.ForeignKey(
        'MarketAttributeValue',
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Attribute value mappings"
        unique_together = [
            'attribute_map',
            'feed_attribute_value',
            'marketplace_attribute_value',
        ]

    def save(self, *args, **kwargs):
        """Save method for ValueMap model."""
        map_feed_attribute_id = self.attribute_map.feed_attribute_id
        feed_attribute_id = self.feed_attribute_value.attribute_id
        if map_feed_attribute_id != feed_attribute_id:
            raise ValidationError("AttributeMap conflict")
        super().save(*args, **kwargs)

    def __str__(self) -> str:
        """Return string representation of AttributeMap model."""
        return f"{self.feed_attribute_value}" \
               f" : {self.marketplace_attribute_value}"

    def get_mapping_data(self) -> MappingData:
        """Get dict with mapping data."""
        category_map = self.attribute_map.category_map
        mapping_data = {
            'mapping_id': self.id,
            'feed_id': category_map.feed_category.feed.id,
            'feed_attribute_value_id': self.feed_attribute_value.id,
            'feed_attribute_value': self.feed_attribute_value.value,
            'marketplace_id': category_map.marketplace_category.marketplace.id,
            'marketplace_attribute_value_id':
            self.marketplace_attribute_value.id,
            'marketplace_attribute_value':
            self.marketplace_attribute_value.value,
            'attribute_mapping_id': self.attribute_map_id,
            'marketplace_attribute_info':
            self.marketplace_attribute_value.info,
            'marketplace_attribute_picture_url':
            self.marketplace_attribute_value.picture_url,
            'deleted': self.marketplace_attribute_value.deleted,
        }

        return mapping_data


class FeedMarketplaceSettings(models.Model):
    """FeedCategorySettings model for mapper.

    :param int marketplace: Marketplace id
    :param int feed: FeedMeta id
    :param bool hidden: True if category should be hidden
    """

    feed = models.ForeignKey(
        'FeedMeta',
        on_delete=models.CASCADE,
    )

    marketplace = models.ForeignKey(
        'Marketplace',
        on_delete=models.CASCADE,
    )

    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    content_object = GenericForeignKey('content_type', 'object_id')

    hidden = models.BooleanField(default=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Feed marketplace settings"
        unique_together = ['feed', 'marketplace', 'content_type', 'object_id']

    def __str__(self):
        """Return string representation of FeedMarketplaceSettings model."""
        return (
            f'{self.marketplace} {self.feed}: '
            f'{self.content_type} [{self.object_id}]'
        )


class ValueUnit(models.Model):
    """ValueUnit model for mapper.

    :param str name: Unit name
    """

    name = models.CharField(max_length=20, unique=True)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Value units"

    def __str__(self):
        """Return string representation of ValueUnit model."""
        return f'{self.name}'


class ValueUnitMap(models.Model):
    """ValueUnitMap model for mapper.

    param int value_unit_from: ValueUnit id
    param int value_unit_to: ValueUnit id
    param float multiplier: Multiplier to convert the feed value
    """

    value_unit_from = models.ForeignKey(
        ValueUnit,
        on_delete=models.CASCADE,
        related_name='fromvaluemap',
    )
    value_unit_to = models.ForeignKey(
        ValueUnit,
        on_delete=models.CASCADE,
        related_name='tovaluemap',
    )
    multiplier = models.FloatField(blank=False, null=False)

    class Meta:
        """Verbose name and unique together."""

        verbose_name_plural = "Value unit maps"
        unique_together = ['value_unit_from', 'value_unit_to']

    def __str__(self) -> str:
        """Return string representation of AttributeMap model."""
        return f'{self.value_unit_from}:{self.value_unit_to}'


class FeedMarketplaceMap(models.Model):
    """FeedMarketplaceMap model for mapper.

    :param int marketplace: Marketplace id
    :param int feed: FeedMeta id
    :param datetime last_sync_date: Last synchronization date
    :param str error: Sync error"""

    feed = models.ForeignKey(
        'FeedMeta',
        on_delete=models.CASCADE,
    )
    marketplace = models.ForeignKey(
        'Marketplace',
        on_delete=models.CASCADE,
    )
    last_sync_date = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Last date of successful synchronization',
    )
    error = models.TextField(blank=True)
