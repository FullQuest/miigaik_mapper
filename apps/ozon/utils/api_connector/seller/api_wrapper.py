"""Wrapper for seller API."""
import logging
import json

from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

from apps.ozon.exceptions import (
    OzonProcessingException,
)

from apps.ozon.utils.api_connector.exceptions import (
    ImportProductRequestSizeLimitExceeded,
    RequestsPerSecondLimitExceeded,
)
from apps.ozon.utils.api_connector.offers_ir import (
    Offer,
    convert_offer_to_item,
)
from apps.ozon.utils.api_connector.connector import (
    OzonSellerConnector,
)
from apps.ozon.utils.api_connector.seller.urls import OzonSellerMethod
from apps.ozon.utils.api_connector.static_data import (
    OFFERS_FILTERS,
)
from apps.utils.iterable_utils import (
    chain_chunks_from_iterable,
)
from apps.utils.retry_utils import retry_on

PRODUCT_VISIBILITY_TYPES = {
    'ALL',
    'VISIBLE',
    'INVISIBLE',
    'EMPTY_STOCK',
    'READY_TO_SUPPLY',
    'STATE_FAILED',
}

log = logging.getLogger('ozon_api_connector')
clickhouse_logger = logging.getLogger('clickhouse_logger')


def get_description_category_tree(
    domain: str,
    language: Optional[str] = None,
    trace_requests: bool = True,
) -> List[Dict[str, Any]]:
    """Get category tree from Ozon API.

    Get category tree. If category id is given, get category subtree for
    this id. Returns russian language by default.

    :param str domain: Domain name in system
    :param str language: 'EN', 'RU' or 'DEFAULT'
        NOTE: Some categories have only one language, and their representation
        won't be affected by this attribute.
    :param bool trace_requests: Option to log request and response

    :return: Category tree from Ozon API
    :rtype: List[Dict[str, Any]]
    """
    action = OzonSellerMethod.GET_DESCRIPTION_CATEGORY_TREE
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {}

    if language:
        params['language'] = language

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    return response['result']


@retry_on(RequestsPerSecondLimitExceeded)
def get_description_attributes(
        domain: str,
        category_id: int,
        type_id: int,
        language: Optional[str],
        trace_requests: bool = True,
) -> List[Dict[str, Any]]:
    """ Get attributes for given category from Ozon API.

    :param str domain: Domain name in system
    :param int category_id: Ozon category id
    :param int type_id: Ozon type id
    :param str language: 'EN' or 'RU' by default
    :param bool trace_requests: Option to log request and response

    :return: List of attribute dictionaries
    :rtype: List[Dict[str, Any]]
    """
    action = OzonSellerMethod.GET_DESCRIPTION_ATTRIBUTE
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {
        'description_category_id': category_id,
        'type_id': type_id,
    }

    if language:
        params['language'] = language

    category_attributes = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    if (
        category_attributes
        and category_attributes
        .get('error', {})
        .get('code') == 'TOO_MANY_REQUESTS'
    ):
        raise RequestsPerSecondLimitExceeded(
            'You have reached request rate limit per second!',
        )

    try:
        category_attributes = category_attributes['result']
    except Exception as err:
        if category_attributes and category_attributes.get('error'):
            log.error(category_attributes.get('error'))
        else:
            log.error(err)
        return []

    return category_attributes


@retry_on(RequestsPerSecondLimitExceeded)
def get_description_attribute_values(
    domain: str,
    category_id: int,
    type_id: int,
    attribute_id: int,
    language: Optional[str] = 'RU',
    last_value_id: Optional[int] = 0,
    limit: Optional[int] = 50,
    trace_requests: bool = True,
) -> Optional[List[Dict[str, Any]]]:
    """ Get attributes values for given category from Ozon API.

    :param str domain: Domain name in system
    :param int category_id: Ozon category id
    :param int type_id: Ozon type id
    :param int attribute_id: Attribute id
    :param str language: 'EN' or 'RU' by default
    :param int last_value_id: Id of attribute to start response with
    :param int limit: Limit of values in response
    :param bool trace_requests: Option to log request and response

    :return: List of category attribute values
    :rtype: Optional[List[Dict[str, Any]]]
    """
    action = OzonSellerMethod.GET_DESCRIPTION_ATTRIBUTE_VALUES
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {
        'description_category_id': category_id,
        'type_id': type_id,
        'attribute_id': attribute_id,
    }

    if last_value_id:
        params['last_value_id'] = last_value_id

    if limit:
        params['limit'] = limit

    if language:
        params['language'] = language

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    if response and 'result' in response:
        category_attribute_values = response

        return category_attribute_values

    return None


def product_import(
    domain: str,
    offers: List[Offer],
    trace_requests: bool = True,
) -> Dict[str, Union[int, List[int], Exception]]:
    """Post product on Ozon or update it`s description.

    This method allows to create or update up to 1000 products.
    Products update puts them to moderation queue,
    so this method should be used to update only certain fields of
    product description and attributes.

    :param domain: Domain name in system
    :param offers: List of Offer dataclass
    :param bool trace_requests: Option to log request and response

    :raises ImportProductRequestSizeLimitExceeded: If you are trying to import
                                                   over 1000 products

    :return product_import_info: Code of product import task on Ozon and
                                 imported offers ID`s
    :rtype: Dict[str, Union[int, List[int]]]
    """
    if len(offers) > 1000:
        raise ImportProductRequestSizeLimitExceeded

    action = OzonSellerMethod.PRODUCT_IMPORT
    connector = OzonSellerConnector(action)

    items: List[Dict[str, Any]] = []

    for offer in offers:

        item = convert_offer_to_item(offer)

        items.append(item)

    params: Dict[str, Any] = {'items': items}

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    for item in items:
        log.info(
            '[product_import][domain=%s][offer_id=%s][%s]',
            domain,
            item['offer_id'],
            json.dumps(item),
        )

    try:
        return {
            'task_id': response['result']['task_id'],
            'offer_ids': [
                item['offer_id'] for item in items
            ],
        }
    except KeyError as err:
        return {
            'error': f'not key {err} in response: {response}',
            'offer_ids': [offer.offer_id for offer in offers],
        }
    except Exception as err:
        log.error(err)
        # NOTE: pass-through exception to store in database on offer import
        return {
            'error': (
                f'err {err} occurred while '
                f'processing response: {response}'
            ),
            'offer_ids': [offer.offer_id for offer in offers],
        }

def get_product_import_info(
    domain: str,
    task_id: int,
    trace_requests: bool = True,
) -> List[Dict[str, Any]]:
    """Get product import status by given Ozon task id.

    This method returns offer id in seller`s system, Ozon product id,
    status of product import, and total count of items.

    :param str domain: Domain name in system
    :param int task_id: Code of product import task
    :param bool trace_requests: Option to log request and params

    :return import_info: Product import information
    :rtype: List[Dict[str, Any]]
    """
    action = OzonSellerMethod.PRODUCT_IMPORT_INFO
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {'task_id': task_id}

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    try:
        import_info = response['result']['items']
    except Exception as err:
        log.warning(err)
        return []

    return import_info


def chain_product_info_list(
    domain: str,
    offer_id: Optional[Iterable[str]] = None,
    product_id: Optional[Iterable[int]] = None,
    sku: Optional[Iterable[Union[int, str]]] = None,
    limit: Optional[int] = 50,
    trace_requests: bool = True,
    sandbox: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Receive product info list.

    :param str domain: Domain name in system
    :param list product_id: Item ids
    :param list sku: Unique product Ozon ids
    :param limit: Request item limit
    :param list offer_id: Product ids in seller`s ERP
    :param bool trace_requests: Option to log request and response
    :param bool sandbox: Option for debugging

    :return: Full info about requested products
    :rtype: Iterator[Dict[str, Any]]
    """
    iterable: Iterable[Union[str, int]]

    if product_id:
        param = 'product_id'
        iterable = product_id
    elif offer_id:
        param = 'offer_id'
        iterable = offer_id
    elif sku:
        param = 'sku'
        iterable = sku
    else:
        return

    params: Dict[str, Union[bool, list]] = {
        'trace_requests': trace_requests,
        'sandbox': sandbox,
    }

    for batch in chain_chunks_from_iterable(iterable, limit, list):
        params[param] = batch

        yield from get_product_info_list(domain, **params)


@retry_on(OzonProcessingException, max_retry_count=7)
def get_product_info_list(
    domain: str,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[int]] = None,
    sku: Optional[List[int]] = None,
    trace_requests: bool = True,
    sandbox: bool = False,
) -> List[Dict[str, Any]]:
    """Receive product info list.

    :param str domain: Domain name in system
    :param list product_id: Item ids
    :param list sku: Unique product Ozon ids
    :param list offer_id: Product ids in seller`s ERP
    :param bool trace_requests: Option to log request and response
    :param bool sandbox: Option for debugging

    :return: Full info about requested products
    :rtype: list
    """
    action = OzonSellerMethod.PRODUCT_INFO_LIST
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {}

    if product_id:
        params['product_id'] = product_id
    elif offer_id:
        params['offer_id'] = offer_id
    elif sku:
        params['sku'] = sku
    else:
        return []

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
        sandbox=sandbox,
    )

    if response is None or 'error' in response or 'items' not in response:
        log.error(f'[PRODUCT_INFO_LIST][ERR][{domain}][{params}][{response}]')
        raise OzonProcessingException(response)

    return response['items']


@retry_on(OzonProcessingException)
def get_all_products_attribute_info(
    domain: str,
    limit: int = 1000,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[int]] = None,
    sku: Optional[List[int]] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    trace_requests: bool = True,
) -> Iterable[Dict[str, Any]]:
    """Get all products with attribute data from Ozon."""
    last_id = None
    last_id_received_once = False

    while True:
        try:
            chunk_result, _total, last_id = get_product_attribute_info(
                domain=domain,
                last_id=last_id,
                offer_id=offer_id,
                product_id=product_id,
                sku=sku,
                limit=limit,
                sort_by=sort_by,
                sort_dir=sort_dir,
                trace_requests=trace_requests,
            )
        except OzonProcessingException as err:
            if last_id_received_once:
                break
            raise OzonProcessingException(err)

        # NOTE: last_id_received_once used to bypass OzonProcessingException,
        #       because when last_id given on last page, ozon will return not
        #       an empty list in response but error code 5: "item not found"
        if last_id:
            last_id_received_once = True

        if chunk_result:
            yield from chunk_result
            if not last_id:
                break
        else:
            break


def get_product_attribute_info(
    domain: str,
    limit: int = 1000,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[int]] = None,
    sku: Optional[List[int]] = None,
    last_id: Optional[str] = None,
    sort_by: Optional[str] = None,
    sort_dir: Optional[str] = None,
    trace_requests: bool = True,
) -> Tuple[List[Dict[str, Any]], int, str]:
    """Receive the list of products with their attributes.

    :param str domain: Domain name in system

    :param Optional[List[str]] offer_id: Filter by offer_id
    :param Optional[List[str]] product_id: Filter by product_id
    :param Optional[List[str]] sku: Filter by sku
    :param str last_id: last id for next request
        NOTE: this attribute in response equals empty string "" when
        offers list provided.
    :param int limit: Filter by the number of products on page.
        NOTE: if offers list provided, it's not limiting the
        number of offers in response.
    :param str sort_by: The parameter by which the products will be sorted.
        Possible values:
        ['id', 'title', 'offer_id', 'spu', 'sku',
         'seller_sku', 'created_at', 'volume', 'price_index']
    :param str sort_dir: Sorting direction. Possible values:
        ["ASC", "asc"] - ascending
        ["DESC", "desc"] - descending
    :param bool trace_requests: Option to log request and response

    :return product_attribute_info: Product list with attribute data
                                    in accordance to used filter
    :rtype: Tuple[List[Dict[str, Any]], int, str]
    """
    action = OzonSellerMethod.PRODUCT_V4_INFO_ATTRIBUTES
    connector = OzonSellerConnector(action)

    params: Dict[str, Any] = {'filter': {}}

    if offer_id:
        params['filter']['offer_id'] = offer_id
    if product_id:
        params['filter']['product_id'] = product_id
    if sku:
        params['filter']['sku'] = sku

    if last_id:
        params['last_id'] = last_id
    if sort_by:
        params['sort_by'] = sort_by
    if sort_dir:
        params['sort_dir'] = sort_dir
    params['limit'] = limit if 0 < limit <= 1000 else 1000

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    if response is None or 'error' in response or 'result' not in response:
        raise OzonProcessingException(response)

    return (
        response['result'],
        response['total'],
        response['last_id'],
    )


def chain_all_products(
    domain: str,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[int]] = None,
    limit: Optional[int] = 1000,
    visibility: Optional[str] = None,
    trace_requests: bool = True,
) -> Iterable[Dict[str, Any]]:
    """Receive all products in chain.

    :rtype: Iterator[Dict[str, Any]]
    """
    last_id = None

    while True:
        offers_list, _total, last_id = get_product_list(
            domain,
            last_id=last_id,
            offer_id=offer_id,
            product_id=product_id,
            limit=limit,
            visibility=visibility,
            trace_requests=trace_requests,
        )

        if offers_list:
            yield from offers_list
        else:
            break


@retry_on(OzonProcessingException)
def get_product_list(
    domain: str,
    offer_id: Optional[List[str]] = None,
    product_id: Optional[List[int]] = None,
    visibility: Optional[str] = None,
    last_id: Optional[int] = None,
    limit: Optional[int] = 1000,
    trace_requests: bool = True,
) -> Tuple[List[Dict[str, Any]], int, str]:
    """Receive the list of products.

    :param str domain: Domain name in system
    :param List[str] offer_id: product offer_id
    :param List[int] product_id: product_id on Ozon
    :param str visibility: Value from OFFERS_FILTERS options.
    :param int last_id: id for next request. Not needed for the first
    :param int limit: Filter by the number of products on page.
                      Default setting is 1000, minimum is 1,
                      maximum is 1000 products on the page
    :param bool trace_requests: Option to log request and response

    :return product_list: Product list in accordance to used filter and
                          total count of products
    :return
        offers_list: offers list on page,
        total: num of all offers for given filter. If no filter given
            num of all offers in shop,
        last_id: last id for next request,
    :rtype Tuple[List[Dict[str, Any]], int, str]
    """
    action = OzonSellerMethod.PRODUCT_V3_LIST
    connector = OzonSellerConnector(action)

    if visibility and visibility not in OFFERS_FILTERS:
        visibility = 'ALL'

    params: Dict[str, Any] = {'filter': {}}

    if offer_id:
        params['filter']['offer_id'] = offer_id
    if product_id:
        params['filter']['product_id'] = product_id
    if visibility:
        params['filter']['visibility'] = visibility

    if last_id:
        params['last_id'] = last_id
    if limit:
        params['limit'] = limit

    response = connector.request_to_ozon(
        domain=domain,
        params=params,
        trace_requests=trace_requests,
    )

    if response is None or 'error' in response or 'result' not in response:
        raise OzonProcessingException(response)

    return (
        response['result']['items'],
        response['result']['total'],
        response['result']['last_id'],
    )
