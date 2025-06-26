"""Mapper utils."""

import operator
from functools import reduce
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple, Union

from django.db.models import QuerySet
from django.db.models.functions import Upper
from django.utils import timezone
from django.db import transaction

from apps.mapper.models import (
    FeedCategory,
    FeedCategoryAttribute,
    FeedMeta,
    Marketplace,
    MarketCategory,
    MarketCategoryAttribute,
    ValueMap,
    CategoryMap,
    AttributeMap,
    MarketAttributeValue,
    FeedCategoryAttributeValue,
    ValueUnitMap,
    FeedMarketplaceMap,
    MarketAttribute,
)

Categories = List[Dict[str, Union[str, int]]]


def get_feed_ids() -> List[int]:
    """Get list of feed ids from MySQL."""
    feed_ids = FeedMeta.objects.filter(
        deleted=False,
        parsed=True,
    ).values_list('id', flat=True)

    return feed_ids


def get_feed_sql_categories(deleted: bool = None) -> List[int]:
    """Get MySQL feed categories"""
    filters: Dict = {}

    if deleted is not None:
        filters['deleted'] = deleted

    sql_categories = FeedCategory.objects.filter(
        **filters).values_list('source_id', flat=True)

    return sql_categories


def get_market_categories(
    marketplace_id: int,
    deleted: Optional[bool] = None,
    leaf: Optional[bool] = None,
) -> QuerySet:
    """Get MySQL market categories"""
    filters: Dict = {'marketplace_id': marketplace_id}

    if deleted is not None:
        filters['deleted'] = deleted

    if leaf is not None:
        filters['leaf'] = leaf

    return MarketCategory.objects.filter(**filters)


def create_market_category_dict(
    marketplace: str,
    name: str,
    disabled: bool,
    source_id: Union[int, str],
    parent_id: Optional[int] = None,
    leaf: Optional[bool] = False,
) -> Dict[str, Any]:
    """Create category dict.

    :param str marketplace: Marketplace name
    :param str name: Category name
    :param bool disabled: Indicates if category creation
    :param int source_id: Category id
    :param int parent_id: Parent category id
    :param bool leaf: True if category is a leaf

    :return category_dict: Dict representation of category
    :rtype: Dict[str, Any]
    """
    category_dict = {
        'marketplace': marketplace,
        'disabled': disabled,
        'source_id': str(source_id),
        'parent_id': str(parent_id) if parent_id is not None else parent_id,
        'name': name,
        'leaf': leaf,
    }

    return category_dict


def create_market_attribute_dict(
    marketplace: str,
    name: str,
    category_source_id: str,
    unit: str,
    max_count: int,
    data_type: int,
    required: bool,
    collection: bool,
) -> Dict[str, Any]:
    """Create attribute dict.

    :param str marketplace: Marketplace name
    :param str name: Attribute name
    :param str category_source_id: Category source id
    :param str unit: Unit type

    :param bool collection: Attribute with dictionary of values
    :param bool required: Required attribute

    :param int max_count: Max count
    :param int data_type: Data type

    :return attribute_dict: Dict representation of attribute
    :rtype: Dict[str, Any]
    """
    attribute_dict = {
        'marketplace': marketplace,
        'name': name,
        'category_source_id': category_source_id,
        'required': required,
        'max_count': max_count,
        'data_type': data_type,
        'unit': unit,
        'collection': collection,
    }

    return attribute_dict


def make_marketplace_category_tree(categories):
    """Make category tree."""
    categories_by_parent = defaultdict(list)
    for category in categories:
        categories_by_parent[category['parent']].append(category)

    def _make_tree(parent_id=None):
        return [
            {**c, 'children': _make_tree(c['id'])}
            for c in categories_by_parent[parent_id]
        ]

    return _make_tree()


def flatten(lst: List[List]) -> List:
    """Flatten list of lists."""
    return reduce(operator.iconcat, lst, [])


def uniquify(lst: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Uniquify dicts in list."""
    res = [
        dict(y) for y
        in {tuple(x.items()) for x in lst}
    ]

    return res


feed_attr_id = int
mp_attr_id = int
attr_map_id = int
value_mapping = Dict[attr_map_id, Dict[feed_attr_id, mp_attr_id]]
unmapped_values = Dict[attr_map_id, Dict[str, List[Tuple[int, str]]]]


@transaction.atomic
def create_val_mappings(
    val_mappings: value_mapping,
):
    """Create value mappings."""
    for map_id, val_mapping in val_mappings.items():
        for feed_val, mp_val in val_mapping.items():

            ValueMap.objects.get_or_create(
                attribute_map_id=map_id,
                feed_attribute_value_id=feed_val,
                marketplace_attribute_value_id=mp_val,
            )


def get_equal_values(unmapped_dict: unmapped_values) -> value_mapping:
    """Get equal values for provided unmapped values."""
    mapping: value_mapping = {}

    for attribute_map_id, values in unmapped_dict.items():

        if not (
            values.get('mp_values')
            and values.get('unmapped_feed_values')
        ):
            continue

        for feed_value in values['unmapped_feed_values']:
            for mp_value in values['mp_values']:

                if feed_value[1] == mp_value[1]:
                    mapping.setdefault(
                        attribute_map_id,
                        {},
                    )[feed_value[0]] = mp_value[0]

                    break

    return mapping


def get_both_values_for_unmapped(feed_id: int) -> unmapped_values:
    """Get dict with unmapped values and MP values for it.

    :param int feed_id: mapper feed id

    :return unmapped_values
    :rtype Dict[int, Dict[str, List[Tuple[int, str]]]]

    return example:
    {
        attribute_map_id1: {
            'mp_values': [(id1, 'value1'), (id2, 'value2')]
            'unmapped_feed_values': [(id1, 'value1'), (id2, 'value2')]
        },
        attribute_map_id2: {
            'mp_values': [(id1, 'value1'), (id2, 'value2')]
            'unmapped_feed_values': [(id1, 'value1'), (id2, 'value2')]
        },
    }
    """
    attr_maps_unfiltered = AttributeMap.objects.filter(
        feed_attribute__category__feed_id=feed_id,
        marketplace_attribute__attribute__map_equal_values=True,
    )

    attr_maps = [
        attr_map for attr_map in attr_maps_unfiltered
        if all([
            attr_map.marketplace_attribute.attribute.map_equal_values,
            attr_map.marketplace_attribute.attribute.dictionary_id,
        ])
    ]

    unmapped_values_dict: unmapped_values = {}
    feed_attr_ids: List[int] = []
    mp_attributes_dict_ids: Dict[int, int] = {}

    for attr_map in attr_maps:
        feed_attr_ids.append(attr_map.feed_attribute_id)  # type: ignore
        mp_attributes_dict_ids[attr_map.id] = (  # type: ignore
            attr_map.marketplace_attribute.attribute.dictionary_id
        )

    mapped_feed_values_ids = ValueMap.objects.filter(
        attribute_map_id__in=mp_attributes_dict_ids.keys(),
    ).values_list('feed_attribute_value', flat=True)

    unmapped_feed_values = FeedCategoryAttributeValue.objects.filter(
        attribute_id__in=feed_attr_ids,
    ).exclude(
        id__in=mapped_feed_values_ids,
    ).annotate(
        value_upper=Upper('value'),
    ).values_list('attribute_id', 'id', 'value_upper')

    for attr_map in attr_maps:
        unmapped_feed_attribute_values = [
            (value_id, value_upper)
            for attr_id, value_id, value_upper in unmapped_feed_values
            if attr_id == attr_map.feed_attribute_id  # type: ignore
        ]
        if unmapped_feed_attribute_values:
            unmapped_values_dict[attr_map.id] = {  # type: ignore
                'unmapped_feed_values': unmapped_feed_attribute_values,
            }

    marketplace_values = MarketAttributeValue.objects.filter(
        dictionary_id__in=mp_attributes_dict_ids.values(),
        deleted=False,
    ).annotate(
        value_upper=Upper('value'),
    ).values_list('dictionary_id', 'id', 'value_upper')

    for attribute_map_id in unmapped_values_dict:

        mp_attribute_values = [
            (value_id, value_upper)
            for dictionary_id, value_id, value_upper in marketplace_values
            if dictionary_id == mp_attributes_dict_ids[attribute_map_id]
        ]
        if not mp_attribute_values:
            continue
        unmapped_values_dict[attribute_map_id]['mp_values'] = (
            mp_attribute_values
        )

    return unmapped_values_dict


def map_attribute_equal_values_v2(feed_id: int):
    """Get equal value mappings."""
    values_for_map = get_both_values_for_unmapped(feed_id)
    equal_values = get_equal_values(values_for_map)
    create_val_mappings(equal_values)


def map_attribute_equal_values(attribute_map_id: int):
    """Map equal feed and marketplace values."""
    attribute_map = AttributeMap.objects.get(id=attribute_map_id)
    marketplace_attribute = attribute_map.marketplace_attribute.attribute
    if not all([
        marketplace_attribute.map_equal_values,
        marketplace_attribute.dictionary_id,
    ]):
        return

    mapped_feed_values_ids = ValueMap.objects.filter(
        attribute_map_id=attribute_map_id,
    ).values_list('feed_attribute_value', flat=True)

    unmapped_feed_values = dict(
        FeedCategoryAttributeValue.objects.filter(
            attribute_id=attribute_map.feed_attribute_id,
        ).exclude(
            id__in=mapped_feed_values_ids,
        ).annotate(
            value_upper=Upper('value'),
        ).values_list('value_upper', 'id'),
    )

    marketplace_values = dict(
        MarketAttributeValue.objects.filter(
            dictionary_id=marketplace_attribute.dictionary_id,
            deleted=False,
        ).annotate(
            value_upper=Upper('value'),
        ).values_list('value_upper', 'id'),
    )

    for feed_value_value, feed_value_id in unmapped_feed_values.items():
        marketplace_value_id = marketplace_values.get(
            feed_value_value,
        ) or marketplace_attribute.default_value_id
        if not marketplace_value_id:
            continue
        ValueMap.objects.create(
            attribute_map_id=attribute_map_id,
            feed_attribute_value_id=feed_value_id,
            marketplace_attribute_value_id=marketplace_value_id,
        )


def pre_create_rich_attribute(category_map_id: int):
    """Pre create rich content attribute."""
    category_map = CategoryMap.objects.get(id=category_map_id)
    market_attribute, _created = MarketAttribute.objects.get_or_create(
        name='Rich-контент JSON',
        source_id='11254',
        is_rich_content=True,
        defaults={
            'data_type': 'String',
        },
    )
    MarketCategoryAttribute.objects.get_or_create(
        category_id=category_map.marketplace_category_id,
        attribute_id=market_attribute.id,
    )


def map_attributes_by_name(category_map_id: int):
    """Map attributes by name."""
    category_map = CategoryMap.objects.get(id=category_map_id)
    pre_create_rich_attribute(category_map_id)

    mapped_marketplace_attribute_ids = AttributeMap.objects.filter(
        category_map_id=category_map_id,
    ).values_list('marketplace_attribute__id', flat=True)

    attributes_need_to_map = dict(
        MarketCategoryAttribute.objects.filter(
            category_id=category_map.marketplace_category_id,
        ).exclude(
            id__in=mapped_marketplace_attribute_ids,
        ).exclude(
            attribute__map_feed_attribute_name__isnull=True,
        ).exclude(
            attribute__map_feed_attribute_name__exact='',
        ).values_list('attribute__map_feed_attribute_name', 'id'),
    )

    if not attributes_need_to_map:
        return

    for name, market_category_attribute_id in attributes_need_to_map.items():
        for feed_attribute_id in FeedCategoryAttribute.objects.filter(
            category_id=category_map.feed_category_id,
            name__iexact=name,
        ).values_list('id', flat=True):
            attribute_map = AttributeMap.objects.create(
                category_map_id=category_map_id,
                feed_attribute_id=feed_attribute_id,
                marketplace_attribute_id=market_category_attribute_id,
            )
            map_attribute_equal_values(attribute_map.id)


def get_category_map(
    feed_domain: str,
    marketplace: str,
) -> Dict[int, Dict[str, int]]:
    """Fetch category map domain and marketplace.

    Example of categories map:

        {'4242':
            {
                'market_category_id': '123123',
                'mapping_id': 321321,
                'market_category_deleted': False,
            }
        }

    Where key is category_id from feed

    :return: category_map
    :rtype: Dict[str, str]
    """
    category_map_queryset = CategoryMap.objects.select_related(
        'feed_category',
        'marketplace_category',
    ).filter(
        feed_category__feed__domain=feed_domain,
        marketplace_category__marketplace__marketplace=marketplace,
    )

    category_map = {
        cat_map.feed_category.source_id:
            {
                'market_category_id':
                    str(cat_map.marketplace_category.source_id),
                'mapping_id':
                    cat_map.pk,
                'market_category_deleted':
                    cat_map.marketplace_category.deleted,
            }
        for cat_map in category_map_queryset
    }

    return category_map


def get_category_attribute_map(
    category_mapping_id: int,
) -> Dict[str, str]:
    """Fetch category attribute map.

    Example of attribute map:

        {
            'color': {  #feed attribute name
                123123: {  #attribute_map id
                    'attribute_source_id': 123456,  #marketplace attribute id
                    'dictionary_id': 321321,
                    'data_type': 'String',
                    'mapping_id': 123123,
                    'values_map': {  #optional
                        'blue/red': [  #feed attribute value
                            {
                                'value': 'blue',
                                'dictionary_value_id': 10001,
                                'deleted': False,
                            },
                            {
                                'value': 'red',
                                'dictionary_value_id': 10003,
                                'deleted': True
                            },
                        ]
                    },
                    'deleted': False,
                    'ignore_data_type': False,
                }
            }
        }

    :return: attribute_map
    :rtype: Dict[str, str]
    """
    attribute_map_queryset = AttributeMap.objects.select_related(
        'feed_attribute',
        'marketplace_attribute__attribute',
    ).filter(
        category_map__pk=category_mapping_id,
        marketplace_attribute__attribute__disabled=False,
    )

    attribute_map_data: Dict[str, Any] = {}
    for attr_map in attribute_map_queryset:
        market_attribute = attr_map.marketplace_attribute.attribute
        feed_attribute = attr_map.feed_attribute
        feed_attribute_name = normalize_string(feed_attribute.name)
        map_data = attribute_map_data.setdefault(
            feed_attribute_name,
            {},
        ).setdefault(
            attr_map.pk,
            {
                'source_id': market_attribute.source_id,
                'dictionary_id': market_attribute.dictionary_id,
                'data_type': market_attribute.data_type,
                'mapping_id': attr_map.pk,
                'from_unit_id': feed_attribute.unit_id,
                'to_unit_id': market_attribute.unit_id,
                'is_rich_content': market_attribute.is_rich_content,
                'ignore_data_type': market_attribute.ignore_data_type,
                'deleted': attr_map.marketplace_attribute.deleted,
            },
        )

        if map_data['dictionary_id']:
            map_data['values_map'] = {}

    value_map_queryset = ValueMap.objects.select_related(
        'attribute_map',
        'feed_attribute_value',
        'marketplace_attribute_value',
        'feed_attribute_value__attribute',
    ).filter(
        attribute_map__category_map__pk=category_mapping_id,
        attribute_map__marketplace_attribute__attribute__disabled=False,
    )

    for val_map in value_map_queryset:
        market_value = val_map.marketplace_attribute_value
        feed_value = val_map.feed_attribute_value
        feed_attribute_name = normalize_string(feed_value.attribute.name)
        attr_values = attribute_map_data[
            feed_attribute_name
        ][val_map.attribute_map_id].get('values_map')
        if attr_values is None:
            continue

        attr_values.setdefault(feed_value.value.upper(), []).append({
            'value': market_value.value,
            'dictionary_value_id': market_value.source_id or 0,
            'deleted': market_value.deleted,
        })

    return attribute_map_data


def get_market_category_attributes(
    marketplace: str,
    category_source_id: Union[int, str],
) -> Dict[str, Dict[str, Any]]:
    """Fetch marketplace category attributes data."""
    category_attribute_queryset = MarketCategoryAttribute.objects.filter(
        category__marketplace__marketplace=marketplace,
        category__source_id=f'{category_source_id}',
        deleted=False,
    ).select_related('attribute')
    attributes = {}
    for cat_attr in category_attribute_queryset:
        attribute = cat_attr.attribute
        attributes[attribute.source_id] = {
            'required': cat_attr.required,
            'disabled': attribute.disabled,
            'dictionary_id': attribute.dictionary_id,
            'name': attribute.name,
        }
    return attributes


def get_unit_map():
    """Retrieve value unit mapping."""
    result = {}
    for unit_map in ValueUnitMap.objects.all():
        result[
            (unit_map.value_unit_from_id, unit_map.value_unit_to_id)
        ] = unit_map.multiplier
        result[
            (unit_map.value_unit_to_id, unit_map.value_unit_from_id)
        ] = 1/unit_map.multiplier
    return result


def get_mapped_values(
    attribute_map: dict,
    feed_attribute_values,
) -> Dict[str, Any]:
    """Get attributes values for marketplace."""
    value_unit_map = get_unit_map()

    result: Dict[str, Any] = {
        'values': {},
        'unmapped_attributes': [],
        'unmapped_value_attributes': [],
        'empty_value_attributes': [],
        'type_error_attributes': [],
        'mapped_with_deleted': [],
        'mapped_with_deleted_value': [],
    }

    for attr_name, attr_values in feed_attribute_values.items():
        for attr_value in attr_values:
            upper_value = attr_value.upper()
            attribute_mapping_data = attribute_map.get(attr_name)
            if not attribute_mapping_data:
                result['unmapped_attributes'].append(attr_name)
                continue

            for mapping_for_market in attribute_mapping_data.values():
                if mapping_for_market['deleted']:
                    continue

                attribute_source_id = mapping_for_market['source_id']

                if attr_value == '':
                    result['empty_value_attributes'].append(
                        attribute_source_id,
                    )
                    continue

                if mapping_for_market['dictionary_id']:
                    values_maps = mapping_for_market['values_map']
                    if upper_value not in values_maps:
                        result['unmapped_value_attributes'].append(
                            attribute_source_id,
                        )
                        continue

                    for values_map in values_maps[upper_value]:
                        if values_map['deleted']:
                            continue
                        result['values'].setdefault(
                            attr_name,
                            [],
                        ).append(
                            dict(
                                **values_map,
                                attribute_source_id=attribute_source_id,
                                data_type=mapping_for_market['data_type'],
                                is_rich_content=mapping_for_market[
                                    'is_rich_content'
                                ],
                                ignore_data_type=(
                                    mapping_for_market['ignore_data_type']
                                ),
                            ),
                        )

                    if attr_name not in result['values']:
                        result['mapped_with_deleted_value'].append(
                            attribute_source_id,
                        )

                else:
                    multiplier = value_unit_map.get(
                        (
                            mapping_for_market['from_unit_id'],
                            mapping_for_market['to_unit_id'],
                        ),
                    )
                    value = attr_value
                    if multiplier:
                        try:
                            num_value = float(value.replace(',', '.'))
                            value = str(round(num_value * multiplier, 10))
                        except ValueError:
                            result['type_error_attributes'].append(
                                attribute_source_id,
                            )
                            continue

                    result['values'].setdefault(
                        attr_name,
                        [],
                    ).append({
                        'value': value,
                        'dictionary_value_id': 0,
                        'attribute_source_id': attribute_source_id,
                        'data_type': mapping_for_market['data_type'],
                        'ignore_data_type':
                            mapping_for_market['ignore_data_type'],
                        'is_rich_content': mapping_for_market[
                            'is_rich_content'
                        ],
                    })
    return result


def update_mapping_sync_date(
    domain: str,
    marketplace: str,
):
    """Set last sync date."""
    feed = FeedMeta.objects.get(domain=domain)
    marketplace = Marketplace.objects.get(marketplace=marketplace)
    feed_marketplace_map, _ = FeedMarketplaceMap.objects.update_or_create(
        feed=feed,
        marketplace=marketplace,
        defaults={
            'last_sync_date': timezone.now(),
            'error': '',
        },
    )


def set_mapping_sync_error(
    domain: str,
    marketplace: str,
    error: str,
):
    """Set mapping sync error."""
    feed = FeedMeta.objects.get(domain=domain)
    marketplace = Marketplace.objects.get(marketplace=marketplace)
    feed_marketplace_map, _ = FeedMarketplaceMap.objects.update_or_create(
        feed=feed,
        marketplace=marketplace,
        defaults={
            'error': error,
        },
    )


def copy_mapping(
    from_feed_id: int,
    to_feed_id: int,
):
    """Copy mappings from feed to other one.

    :param int from_feed_id: From feed id
    :param int to_feed_id: New feed id
    """
    new_category_ids = {}

    def get_new_category_id(old_category: FeedCategory):
        if old_category.id not in new_category_ids:
            old_category.feed_id = to_feed_id
            old_category_id = old_category.id
            old_category.id = None
            old_category.save()
            new_category_ids[old_category_id] = old_category.id
            return old_category.id
        return new_category_ids[old_category.id]

    for category_map in CategoryMap.objects.filter(
        feed_category__feed_id=from_feed_id,
    ):
        from_category_map_id = category_map.id

        category_map.feed_category_id = get_new_category_id(
            category_map.feed_category,
        )
        category_map.id = None
        category_map.save()

        new_feed_attributes = {}
        for attribute_map in AttributeMap.objects.filter(
            category_map_id=from_category_map_id,
        ):
            from_attribute_map_id = attribute_map.id

            if attribute_map.feed_attribute_id not in new_feed_attributes:
                attribute = attribute_map.feed_attribute
                from_attribute_id = attribute.id
                attribute.category_id = category_map.feed_category_id
                attribute.id = None
                attribute.save()
                new_feed_attributes[from_attribute_id] = attribute.id

            new_attribute_id = new_feed_attributes[
                attribute_map.feed_attribute_id
            ]
            attribute_map.feed_attribute_id = new_attribute_id
            attribute_map.category_map = category_map
            attribute_map.id = None
            attribute_map.save()

            new_feed_values = {}
            for value_map in ValueMap.objects.filter(
                attribute_map_id=from_attribute_map_id,
            ):
                if value_map.feed_attribute_value_id not in new_feed_values:
                    value = value_map.feed_attribute_value
                    from_value_id = value.id
                    value.attribute_id = new_attribute_id
                    value.id = None
                    value.save()
                    new_feed_values[from_value_id] = value.id

                new_value_id = new_feed_values[
                    value_map.feed_attribute_value_id
                ]

                value_map.feed_attribute_value_id = new_value_id
                value_map.attribute_map = attribute_map
                value_map.id = None
                value_map.save()


def normalize_string(string):
    """Normalize string for param names."""
    return string.strip().upper().replace('\u00A0', ' ')
