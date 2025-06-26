"""Models for Ozon app."""
import re
from typing import Iterable

from django.db import models

from apps.mapper.models import FeedMeta
from apps.utils.transliterate_utils import tr_from_rus_to_eng


class OzonAuthKey(models.Model):
    """OzonAuthKey model.

    Table for storing information of granted API key and parameters for Ozon
    shop for t-unit app in Ozon.

    :param str domain: Unique domain name in system
    :param str client_id: Client id for Ozon API
    :param str api_key: API key for Ozon
    :param str logs: Logs

    :param bool enable_posting: Enable posting
    :param bool full_update_allowed: True if full domain update allowed
    :param bool enable_daily_offers_update: Force daily offers update
    """
    domain = models.CharField(max_length=50, primary_key=True, db_index=True)
    client_id = models.CharField(max_length=100)
    api_key = models.CharField(max_length=100)

    is_disabled = models.BooleanField(
        default=False,
        help_text='Выключение домена',
    )
    enable_posting = models.BooleanField(
        default=True,
        help_text='Постинг товаров',
    )
    full_update_allowed = models.BooleanField(
        default=False,
        help_text='Апдейт цен, остатков и карточек',
    )
    enable_daily_offers_update = models.BooleanField(
        default=False,
        help_text='Дополнительное ежедневное обновление карточек',
    )

    mapping_from_domain = models.ForeignKey(
        'OzonAuthKey',
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        help_text='Домен, маппинг которого будет использоваться для постинга'
    )

    logs = models.TextField(blank=True, null=True, editable=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = "Domains & API keys"

    def __str__(self) -> str:
        """Return String represenation of OzonAuthKey model."""
        return self.domain


class OzonFeedUrl(models.Model):
    """FeedUrl model for Ozon.

    :param str domain: Domain of the company
    :type domain: :class:`OzonDomain`
    :param str url: Url of feed file on server
    :param str login: Feed login
    :param str password: Feed password
    :param int parsed: Parsed status
    :param int updated: Updated status
    :param str collection_name: Collection in mongo
    :param str error: Error string
    :param int deleted: Deleted state
    :param str custom_name: Custom name for quick search
    :param datetime last_parsed_date: Last date of successful parsing
    """

    domain = models.OneToOneField(
        'ozon.OzonAuthKey',
        to_field='domain',
        on_delete=models.CASCADE,
        primary_key=True,
    )
    url = models.TextField()

    parsed = models.BooleanField(default=False)
    updated = models.BooleanField(default=False)
    deleted = models.BooleanField(default=False)

    collection_name = models.CharField(
        max_length=50,
        blank=True,
        null=True,
        unique=True,
        editable=False,
    )
    error = models.CharField(max_length=50, blank=True, null=True)
    custom_name = models.CharField(max_length=100, blank=True, null=True)
    login = models.CharField(max_length=50, blank=True, null=True)
    password = models.CharField(max_length=50, blank=True, null=True)

    timestamp_modified = models.DateTimeField(auto_now=True)
    last_parsed_date = models.DateTimeField(
        null=True,
        blank=True,
        help_text='Last date of successful feed parse',
    )
    parsed_hash = models.TextField(blank=True, null=True, editable=False)

    class Meta:
        """Verbose name."""

        verbose_name_plural = 'Feed URLs'

    def save(self, *args, **kwargs):
        """Set collection name."""
        super().save(*args, **kwargs)

        if not self.collection_name:
            # Transliterate domain. Alphanumeric, underscore, dots whitelist
            tr_domain = tr_from_rus_to_eng(self.domain.domain)
            self.collection_name = re.sub(r'[^\w\.]', '', tr_domain)
            super().save(*args, **kwargs)

        mapper_feed, _ = FeedMeta.objects.update_or_create(
            domain=self.domain,
            defaults={
                'url': self.url,
            },
        )


class OzonOffer(models.Model):
    """Ozon offers model.

    Posted Ozon offers.

    :param str domain: Domain of the company
    :type domain: :class:`OzonDomain`

    :param str feed_offer_id: Offer ID from feed
    :param str state: Current product status in Ozon system
    :param str last_import_request_data: Last import data sent to Ozon
    :param str errors: Errors
    :param str last_import_hash: Last import hash

    :param int product_id: Product ID from Ozon
    :param int task_id: Product import task ID

    :param bool is_imported: Offer was imported
    :param bool is_processed: True if product is processed by Ozon system
    """

    PROCESSING = "processing"
    MODERATING = "moderating"
    PROCESSED = "processed"
    FAILED_MODERATION = "failed_moderation"
    FAILED_VALIDATION = "failed_validation"
    FAILED = "failed"

    STATE_CHOICES = (
        (PROCESSING, PROCESSING),
        (MODERATING, MODERATING),
        (PROCESSED, PROCESSED),
        (FAILED_MODERATION, FAILED_MODERATION),
        (FAILED_VALIDATION, FAILED_VALIDATION),
        (FAILED, FAILED),
    )
    domain = models.ForeignKey(
        'OzonAuthKey',
        to_field='domain',
        on_delete=models.CASCADE,
    )
    feed_offer_id = models.CharField(max_length=50, db_index=True)
    state = models.CharField(max_length=30, choices=STATE_CHOICES)

    last_import_request_data = models.TextField(null=True, blank=True)
    errors = models.TextField(blank=True, null=True)
    last_import_hash = models.TextField(blank=True, null=True, editable=False)

    product_id = models.PositiveIntegerField(
        blank=True,
        null=True,
        db_index=True,
    )
    task_id = models.BigIntegerField(null=True, blank=True)

    is_imported = models.BooleanField(default=False)
    is_processed = models.BooleanField(default=False)

    last_offer_update = models.DateTimeField(blank=True, null=True)

    class Meta:
        """Verbose name and constraints."""

        verbose_name_plural = 'Offers'
        unique_together = ('domain', 'feed_offer_id')
