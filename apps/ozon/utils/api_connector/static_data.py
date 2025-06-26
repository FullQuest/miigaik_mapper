"""Module with static data for OZON api requests."""

OFFERS_FILTERS = [
    'ALL',  # all products except archived ones.
    'VISIBLE',  # products visible to customers.
    'INVISIBLE',  # products not visible to customers for some reason.
    'EMPTY_STOCK',  # products with no stocks specified.
    'NOT_MODERATED',  # products that didn't pass moderation.
    'MODERATED',  # products that passed moderation.
    'DISABLED',  # products visible to customers but not available for purchase.            # noqa: E501
    'STATE_FAILED',  # products, which creation ended up with an error.
    'READY_TO_SUPPLY',  # products ready for shipment.
    'VALIDATION_STATE_PENDING',  # products that are being pre-moderated (by a validator).  # noqa: E501
    'VALIDATION_STATE_FAIL',  # products that didn't pass pre-moderation (by a validator).  # noqa: E501
    'VALIDATION_STATE_SUCCESS',  # products that passed pre-moderation (by a validator).    # noqa: E501
    'TO_SUPPLY',  # products ready for sale.
    'IN_SALE',  # products on sale.
    'REMOVED_FROM_SALE',  # products hidden from customers.
    'BANNED',  # blocked products.
    'OVERPRICED',  # overpriced products.
    'CRITICALLY_OVERPRICED',  # critically overpriced products.
    'EMPTY_BARCODE',  # products without a barcode.
    'BARCODE_EXISTS',  # products with a barcode.
    'QUARANTINE',  # products in quarantine after a price change for more than 50%.       # noqa: E501
    'ARCHIVED',  # archived products.
    'OVERPRICED_WITH_STOCK',  # products on sale with a price higher than other sellers.  # noqa: E501
    'PARTIAL_APPROVED',  # products on sale with blank or incomplete descriptions.        # noqa: E501
    'IMAGE_ABSENT',  # products without images.
    'MODERATION_BLOCK',  # products for which moderation is blocked.
]
