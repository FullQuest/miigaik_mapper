"""Populate mysql with feed attributes."""

import os
import sys
import django
import logging
import argparse

from multiprocessing import Pool
from typing import Any, Dict, List, Union, Optional
from pymongo.errors import AutoReconnect

from django.db.models.functions import Upper
from django.db import transaction
from django.db.utils import OperationalError
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.mapper.models import (        # noqa: E402
    FeedCategory,
    FeedCategoryAttribute,
    FeedCategoryAttributeValue,
    AttributeMap,
    ValueUnit,
)
from apps.mapper.utils.utils import (   # noqa: E402
    flatten,
    get_feed_ids,
    uniquify,
    map_attribute_equal_values,
    map_attribute_equal_values_v2,
)
from apps.utils import MongoConnMixin, run_checker      # noqa: E402
from apps.utils.retry_utils import retry_on             # noqa: E402
from apps.utils.iterable_utils import split_to_chunks   # noqa: E402


log = logging.getLogger(__name__)

AttributeData = Dict[str, Union[int, str]]
AttributesData = List[AttributeData]

EMPTY_PARAM_DATA = {
    '@name': 'NO PARAMS PROVIDED',
}

SAVE_VALUES_TAGS = [
    'vendor',
    'country_of_origin',
]


@retry_on(AutoReconnect)
def fetch_feed_category_attributes(feed_id: int) -> AttributesData:
    """Fetch attributes from Mongo by feed_id.

    :param int feed_id: Feed id

    :return feed_attributes_data: List of dict repr of attributes from Mongo
    :rtype: AttributesData

    AttributeData example: {
        'feed_id': 56,
        'category_id': 822991,
        'name': 'Дополнительный доступ внутрь',
        'unit': None,
        'value': 'нижний',
    }
    """
    collection = f'feed_{feed_id}'

    conn = MongoConnMixin(
        dbname='mapper',
        collection=collection,
        subcollection='offers',
    )

    offers = conn.collection.find({}, {'_id': False})

    feed_attributes = []

    for offer in offers:
        offer_params = offer.get('param')

        if not offer_params:
            offer_params = EMPTY_PARAM_DATA

        if not isinstance(offer_params, list):
            offer_params = [offer_params]

        attributes_data = [
            {
                'feed_id': feed_id,
                'category_id': offer['categoryId'],
                'name': offer_param['@name'],
                'unit': offer_param.get('@unit'),
                'value': offer_param.get('#text', ''),
            }
            for offer_param in offer_params
            if offer_param
        ]

        tags_data = []
        for tag, values in offer.items():
            if values is None:
                continue
            elif not isinstance(values, list):
                values = [values]

            tag_data = [
                {
                    'feed_id': feed_id,
                    'category_id': offer['categoryId'],
                    'name': tag,
                    'value': value,
                    'is_tag': True,
                }
                for value in values
                if isinstance(value, (str, int, float, bool))
            ]
            tags_data.extend(tag_data)

        feed_attributes.append(attributes_data + tags_data)

    flat_feed_attributes = flatten(feed_attributes)
    feed_attributes_data = uniquify(flat_feed_attributes)

    return feed_attributes_data


@retry_on(OperationalError)
def get_attributes_categories(
    attribute_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Get and add FeedCategory object into each attribute dict."""
    feed_ids = list({attr['feed_id'] for attr in attribute_data})
    category_ids = list({attr['category_id'] for attr in attribute_data})

    categories = FeedCategory.objects.filter(
        feed_id__in=feed_ids,
        source_id__in=category_ids,
    )

    attributes_with_categories: List[Dict[str, Any]] = []

    for attr in attribute_data:
        for category in categories:
            if (
                category.source_id == attr['category_id']
                and category.feed_id == attr['feed_id']  # type: ignore
            ):
                attr['category_obj'] = category
                attributes_with_categories.append(attr)
                break

    return attributes_with_categories


def get_attributes_units(
    attribute_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Get or create and add ValueUnit object into each attribute dict."""
    units_values = ValueUnit.objects.all().annotate(
        unit_uppercase=Upper('name'),
    ).values_list('unit_uppercase', flat=True)

    attribute_units = list({
        attr['unit'] for attr in attribute_data
        if attr.get('unit')
    })

    value_units_to_create = [
        attr_unit.strip() for attr_unit in attribute_units
        if attr_unit.upper().strip() not in units_values
    ]

    if value_units_to_create:
        for attr_name in value_units_to_create:
            ValueUnit.objects.create(name=attr_name)

    units = ValueUnit.objects.all().annotate(unit_uppercase=Upper('name'))

    for attr in attribute_data:
        unit_name = (attr.get('unit') or '').strip()
        if not unit_name and ',' in attr['name']:
            unit_name = attr['name'].split(',')[-1].strip()

        if unit_name:
            for unit in units:
                if unit_name.upper() == unit.unit_uppercase:  # type: ignore
                    attr['unit_obj'] = unit
                    break

    return attribute_data


def remove_dict_duplicates_by_keys(
    input_dicts: List[dict],
    keys: List[str],
) -> List[dict]:
    """Remove dict duplicates for specific keys."""
    values_tuple = []
    dicts_no_duplicates: List[dict] = []
    for input_dict in input_dicts:
        cur_tuple = tuple(
            val for key, val in input_dict.items() if key in keys
        )

        if 'attribute' in input_dict:
            cur_tuple = (cur_tuple[0], cur_tuple[1].upper())

        if cur_tuple not in values_tuple:
            values_tuple.append(cur_tuple)
            dicts_no_duplicates.append(input_dict)
    return dicts_no_duplicates


def standardize_str(convert: str) -> str:
    """Convert string to comparison format."""
    return convert.upper().replace('Ё', 'Е').replace('\xa0', ' ')


def update_or_create_attributes(
    attribute_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Get or create attributes."""
    all_attr_names = [attr['name'] for attr in attribute_data]
    all_attr_categories = [attr['category_obj'] for attr in attribute_data]

    attribute_objects = FeedCategoryAttribute.objects.filter(
        category__in=all_attr_categories,
        name__in=all_attr_names,
    )
    attrs_to_update: List[Dict[str, Any]] = []
    attrs_to_create: List[Dict[str, Any]] = []
    all_attrs: List[Dict[str, Any]] = []

    attr_objects_dict = {
        (
            obj.category_id,   # type: ignore
            standardize_str(obj.name),
            obj.is_tag,
        ): {
            'obj': obj,
            **obj.__dict__,
        }
        for obj in attribute_objects
    }

    for attr in attribute_data:

        attr_tuple = (
            attr["category_obj"].id,
            standardize_str(attr["name"]),
            attr.get("is_tag", False),
        )

        if attr_tuple in attr_objects_dict:

            unit_id = getattr(attr.get('unit_obj'), 'id', None)
            deleted = attr.get('deleted', False)

            if all([
                attr_objects_dict[attr_tuple]['unit_id'] == unit_id,
                attr_objects_dict[attr_tuple]['deleted'] == deleted,
            ]):
                attr['attribute_obj'] = attr_objects_dict[attr_tuple]['obj']
                all_attrs.append(attr)
            else:
                attr['attribute_obj'] = attr_objects_dict[attr_tuple]['obj']
                attrs_to_update.append(attr)
        else:
            attrs_to_create.append(attr)

    attrs_no_duplicates = remove_dict_duplicates_by_keys(
        attrs_to_create,
        ['category_obj', 'name', 'is_tag'],
    )
    with transaction.atomic():
        for attr in attrs_no_duplicates:
            feed_attribute = FeedCategoryAttribute.objects.create(
                category=attr['category_obj'],
                name=attr['name'],
                is_tag=attr.get("is_tag", False),
                unit=attr.get("unit_obj", None),
                deleted=attr.get("deleted", False),
            )
            attr['attribute_obj'] = feed_attribute
            all_attrs.append(attr)

    with transaction.atomic():
        for attr in attrs_to_update:
            feed_attr_obj: FeedCategoryAttribute = attr['attribute_obj']
            feed_attr_obj.unit = attr.get("unit_obj", None)
            feed_attr_obj.deleted = attr.get("deleted", False)
            feed_attr_obj.save()
            attr['attribute_obj'] = feed_attr_obj

            all_attrs.append(attr)

    return all_attrs


def get_or_create_values(
    attribute_data: List[Dict[str, Any]],
):
    """Get or create attribute values."""
    values_to_create = []
    chunks = split_to_chunks(attribute_data, 5000)
    for chunk in chunks:
        attribute_objects = [attr['attribute_obj'] for attr in chunk]
        attribute_values = [attr['value'] for attr in chunk]

        attribute_values_query = FeedCategoryAttributeValue.objects.filter(
            attribute__in=attribute_objects,
            value__in=attribute_values,
        ).values_list('attribute', 'value')

        created_attribute_values = [
            (attribute[0], standardize_str(attribute[1]))
            for attribute in attribute_values_query
        ]

        for attr in chunk:

            if attr.get('is_tag') and not all([
                attr['name'] in SAVE_VALUES_TAGS,
                isinstance(attr['value'], str),
            ]):
                continue

            attr_tuple = (
                attr['attribute_obj'].id,
                standardize_str(attr['value']),
            )

            if attr_tuple not in created_attribute_values:
                value_data = {
                    'attribute': attr['attribute_obj'],
                    'value': attr['value'],
                }
                values_to_create.append(value_data)

    values_no_duplicates = remove_dict_duplicates_by_keys(
        values_to_create,
        ['attribute', 'value'],
    )

    with transaction.atomic():
        for value_data in values_no_duplicates:
            FeedCategoryAttributeValue.objects.create(**value_data)


@retry_on(OperationalError)
def populate_database_v2(
    attribute_data: List[AttributeData],
):
    """Update or create FeedCategoryAttribute objects.

    :param attribute_data: List of Dict representation of category attribute
    :type attribute_data: List[AttributeData]
    """
    attributes_with_categories = get_attributes_categories(attribute_data)
    attributes_with_units = get_attributes_units(attributes_with_categories)
    attributes_with_their_objects = (
        update_or_create_attributes(attributes_with_units)
    )
    get_or_create_values(attributes_with_their_objects)


def populate_database(
    attribute_data: AttributeData,
    deleted: bool = False,
):
    """Update or create FeedCategoryAttribute object, and set "deleted" flag.

    :param attribute_data: Dict representation of category attribute
    :type attribute_data: AttributeData

    :param bool deleted: True if category attribute needs to be
                         flagged as deleted
    """
    category = FeedCategory.objects.get(
        source_id=attribute_data['category_id'],
        feed=attribute_data['feed_id'],
    )

    unit = None
    unit_name = (attribute_data.get('unit') or '').strip()  # type: ignore
    if unit_name:
        unit = ValueUnit.objects.filter(
            name__iexact=attribute_data['unit'],
        ).first()
        if not unit:
            unit = ValueUnit.objects.create(name=unit_name)
    elif ',' in attribute_data['name']:  # type: ignore
        probably_unit_name = (
            attribute_data['name'].split(',')[-1].strip()  # type: ignore
        )

        unit = ValueUnit.objects.filter(
            name__iexact=probably_unit_name,
        ).first()

    data = {
        'category': category,
        'name': attribute_data['name'],
        'is_tag': attribute_data.get('is_tag', False),
        'defaults': {
            'unit': unit,
            'deleted': deleted,
        },
    }

    attribute, _ = FeedCategoryAttribute.objects.update_or_create(**data)

    if attribute_data.get('is_tag') and not all([
        attribute_data['name'] in SAVE_VALUES_TAGS,
        isinstance(attribute_data['value'], str),
    ]):
        return

    value_data = {
        'attribute': attribute,
        'value': attribute_data['value'],
    }

    FeedCategoryAttributeValue.objects.get_or_create(**value_data)


def map_values(feed_id):
    """Map equal values for feed."""
    for attribute_map in AttributeMap.objects.filter(
        feed_attribute__category__feed_id=feed_id,
        marketplace_attribute__attribute__map_equal_values=True,
    ):
        map_attribute_equal_values(attribute_map.id)  # type: ignore


def feed_attributes_fetcher_main(custom_feed_id: Optional[int]):
    """Fetch feed category attributes from MongoDB and populate MySQL.

    Sets flag "deleted" to attribute if it was removed from feed,
    and restores it, if it was added again.

    :return result: Result of operation
    :rtype: Dict[str, str]
    """
    feed_ids = get_feed_ids()

    if custom_feed_id:

        if custom_feed_id not in feed_ids:
            raise Exception('Feed id not found')

        mongo_attributes = fetch_feed_category_attributes(custom_feed_id)
        feed_ids = [custom_feed_id]

    else:

        with Pool(processes=5) as pool:
            attributes_data = pool.map(
                fetch_feed_category_attributes,
                feed_ids,
            )

        mongo_attributes: AttributesData = flatten(attributes_data)

    print(f'num of attributes: {len(mongo_attributes)}')

    chunks = split_to_chunks(mongo_attributes, 50000)
    for chunk in chunks:
        populate_database_v2(list(chunk))

    for feed_id in feed_ids:
        map_attribute_equal_values_v2(feed_id)


def parse_args(args: List[str]):
    """Parse cmd-line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--feed-id', '-f',
        help='Feed id for processing',
        action='store',
        default=None,
        type=int,
    )

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_args(sys.argv[1:])

    feed_id = args.feed_id

    with run_checker('MapperFeedAttributesFetcher'):
        setproctitle('MapperFeedAttributesFetcher')
        feed_attributes_fetcher_main(feed_id)
