"""Django admin display."""
from django.conf import (
    settings,
)
from django.contrib import (
    admin,
)
from django.db.models import (
    BooleanField,
)
from apps.ozon.models import (
    OzonAuthKey,
    OzonFeedUrl,
    OzonOffer,
)

class ProductionReadonlyMixin:
    """ProductionReadonlyMixin settings for admin models."""

    def has_view_permission(self, *_, **__) -> bool:
        """View permission is always True."""
        return True

    def get_readonly_fields(self, *args, **kwargs):
        """Return read-only fields on debug and all fields on production."""
        if settings.DEBUG:
            # NOTE: that class used only as mixin
            # NOTE: so, mypy can't resolve this method
            return super().get_readonly_fields(*args, **kwargs)  # type: ignore

        # remove duplicates and concatenate with default readonly
        return list(set(
            [field.name for field in self.model._meta.fields]
            + list(super().get_readonly_fields(*args, **kwargs))))

    def has_add_permission(self, *_, **__) -> bool:
        """Add permission True only in debug mode."""
        return settings.DEBUG

    def has_delete_permission(self, *_, **__) -> bool:
        """Delete permission True only in debug mode."""
        return settings.DEBUG


@admin.register(OzonFeedUrl)
class OzonFeedUrlAdmin(admin.ModelAdmin):
    """Admin model for ozon feed urls."""

    list_display = [
        'domain',
        'url',
        'parsed',
        'updated',
        'deleted',
        'error',
        'last_parsed_date',
    ]
    readonly_fields = [
        'parsed',
        'updated',
        'deleted',
        'error',
        'last_parsed_date',
        'collection_name',
    ]


class OzonFeedUrlInline(admin.StackedInline):
    """Inline model for OzonAuthKeyAdmin."""

    model = OzonFeedUrl
    list_display = [
        'parsed',
        'updated',
        'deleted',
        'error',
        'last_parsed_date',
        'collection_name',
    ]
    readonly_fields = [
        'parsed',
        'updated',
        'deleted',
        'error',
        'last_parsed_date',
        'collection_name',
    ]


@admin.register(OzonAuthKey)
class OzonAuthKeyAdmin(admin.ModelAdmin):
    """Admin model for Ozon Auth Keys."""

    change_form_template = "ozon/ozon_auth_key.html"

    search_fields = [
        'domain',
    ]
    list_display = [
        'domain',
        'is_disabled',
        'enable_posting',
        'full_update_allowed',
        'logs',
    ]
    readonly_fields = [
        'logs',
    ]
    list_filter = ('is_disabled',)
    inlines = [
        OzonFeedUrlInline,
    ]

    def get_readonly_fields(self, request, obj=None):
        """Return boolean fields as read-only."""
        default_fields = list(super().get_readonly_fields(request, obj=obj))
        if request.user.is_superuser or not obj:
            return default_fields

        readonly_bool = [
            field.name
            for field in self.model._meta.fields
            if isinstance(field, BooleanField)]

        return set(readonly_bool + default_fields)


@admin.register(OzonOffer)
class OzonOfferAdmin(ProductionReadonlyMixin, admin.ModelAdmin):
    """Read-only admin model for ozon offers."""

    list_display = [
        'domain',
        'feed_offer_id',
        'product_id',
        'is_imported',
        'errors',
        'last_offer_update',
    ]
    search_fields = [
        'domain__domain',
        'feed_offer_id',
        'product_id',
        'errors',
        'state',
    ]
    list_filter = [
        'domain',
        'state',
    ]


