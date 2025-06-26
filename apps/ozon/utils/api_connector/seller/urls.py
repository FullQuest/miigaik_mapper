from dataclasses import dataclass
from typing import NamedTuple

SELLER_API_URL = 'https://api-seller.ozon.ru'
SANDBOX_URL = 'http://cb-api.ozonru.me'
SANDBOX_SETTINGS = {
    'client_id': 836,
    'api_key': '0296d4f2-70a1-4c09-b507-904fd05567b9',
}


class MethodMeta(NamedTuple):
    """Meta info class for url."""

    url: str
    request_type: str = 'POST'


@dataclass
class OzonSellerMethod:
    """Ozon seller API methods."""

    # CATEGORIES
    GET_DESCRIPTION_CATEGORY_TREE = MethodMeta('/v1/description-category/tree')
    GET_DESCRIPTION_ATTRIBUTE = MethodMeta('/v1/description-category/attribute')
    GET_DESCRIPTION_ATTRIBUTE_VALUES = MethodMeta('/v1/description-category/attribute/values')
    # CATEGORIES END

    # PRODUCTS
    PRODUCT_IMPORT = MethodMeta('/v3/product/import')
    PRODUCT_IMPORT_INFO = MethodMeta('/v1/product/import/info')
    PRODUCT_INFO = MethodMeta('/v2/product/info')
    PRODUCT_V4_INFO_ATTRIBUTES = MethodMeta('/v4/product/info/attributes')
    PRODUCT_INFO_LIST = MethodMeta('/v3/product/info/list')
    PRODUCT_V3_LIST = MethodMeta('/v3/product/list')
    # PRODUCTS END

    # REPORTS
    PRODUCTS_REPORT = MethodMeta('/v1/report/products/create')
    # REPORTS END
