"""Ozon attributes fetcher."""

import logging
import os
import sys
from typing import Any, Dict, List, Union

import django
from django.db import transaction
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.mapper.models import (  # noqa: e402
    Marketplace,
    MarketCategory,
    MarketAttribute,
    MarketAttributeValueDictionary,
    MarketCategoryAttribute,
    ValueUnit,
)
from apps.mapper.utils.utils import (  # noqa: e402
    flatten,
    get_market_categories,
)
from apps.ozon.utils.api_connector.seller.api_wrapper import (  # noqa: e402
    get_description_attributes,
)
from apps.utils import run_checker  # noqa: e402
from apps.utils.iterable_utils import split_to_chunks  # noqa: e402

log = logging.getLogger(__name__)
mapper_fetched_dicts_log = logging.getLogger('mapper_fetched_dicts')
mapper_saved_dicts_log = logging.getLogger('mapper_saved_dicts')
mapper_new_dicts_log = logging.getLogger('mapper_new_dicts')
mapper_fetched_attrs_log = logging.getLogger('mapper_fetched_attrs')
mapper_saved_attrs_log = logging.getLogger('mapper_saved_attrs')

DOMAIN = 'www.b2basket.ru'
OZON = 'ozon'
SHARED_DICTIONARY_IDS = [
    3,
    4,  # серия, линейка
    5,
    6,
    7,  # издательство
    8,  # организация
    9,  # фио, автор, персонаж и тд
    28732849,  # brands
    81097892,  # textile code
    7187521,  # коммерческий тип
    8656592,  # длинна стельки мото
    1219,  # длина стельки
]
YOUTUBE_CODE_ATTRIBUTE_ID = 4074

Attribute = Dict[str, Any]
Attributes = List[Attribute]


def is_rich_content(attribute: Dict[str, Any]) -> bool:
    """Return whether the attribute value is rich."""
    apper_name = attribute['name'].upper()
    return 'JSON' in apper_name or 'RICH' in apper_name


def fetch_ozon_category_attributes(source_ids: List[str]) -> Attributes:
    """Fetch attributes data from Ozon and prepare it."""

    if '_' not in source_ids[0]:
        log.error(f'Get old category without type_id: {source_ids[0]}')
        return []

    category_id, type_id = source_ids[0].split('_')

    attributes = get_description_attributes(
        domain=DOMAIN,
        category_id=int(category_id),
        type_id=int(type_id),
        language=None,
    )

    result = []

    for attribute in attributes:
        attribute['source_category_id'] = source_ids[0]
        attribute['id'] = str(attribute['id'])

        if attribute['dictionary_id']:
            if (
                attribute['dictionary_id'] not in SHARED_DICTIONARY_IDS
            ):
                attribute_source_id = (
                    f'{attribute["source_category_id"]}_'
                    f'{attribute["id"]}'
                )
                attribute['dictionary_id'] = attribute_source_id
            else:
                attribute['dictionary_id'] = str(
                    attribute['dictionary_id'],
                )

        result.append(attribute)

    return result


def get_attributes():
    """Get prepared Ozon attributes data."""
    market_types = get_market_categories(
        marketplace_id=Marketplace.objects.get(marketplace=OZON).pk,
        deleted=False,
        leaf=True,
    )

    attributes = map(
        fetch_ozon_category_attributes,
        split_to_chunks(
            market_types.values_list('source_id', flat=True),
            1,
        ),
    )

    flat_attributes: Attributes = flatten(attributes)

    attribute_data: Dict[str, Union[set, list]] = {
        'dictionary_ids': {
            attribute['dictionary_id']
            for attribute
            in flat_attributes
        },
        'attributes': flat_attributes,
    }

    return attribute_data


def populate_database(attribute_data):
    """Populate attribute data into the database."""
    fetched_dictionary_ids = set(filter(
        bool,
        attribute_data['dictionary_ids'],
    ))

    mapper_fetched_dicts_log.info(fetched_dictionary_ids)

    saved_dictionaries = {
        values['attribute__dictionary__source_id']:
            values['attribute__dictionary_id']
        for values in MarketCategoryAttribute.objects.filter(
            category__marketplace__marketplace=OZON,
        ).values(
            'attribute__dictionary_id',
            'attribute__dictionary__source_id',
        )
    }

    saved_dictionary_ids = set(saved_dictionaries)

    mapper_saved_dicts_log.info(saved_dictionary_ids)

    new_dictionary_ids = list(fetched_dictionary_ids - saved_dictionary_ids)

    mapper_new_dicts_log.info(new_dictionary_ids)

    for chunk in split_to_chunks(
        new_dictionary_ids,
        5000,
    ):
        with transaction.atomic():
            for dictionary_source_id in chunk:
                dictionary_id = \
                    MarketAttributeValueDictionary.objects.create(
                        source_id=dictionary_source_id,
                    ).id
                saved_dictionaries[dictionary_source_id] = dictionary_id

    categories = {
        values['source_id']: values['id']
        for values in MarketCategory.objects.filter(
            marketplace__marketplace=OZON,
        ).values('id', 'source_id')
    }

    saved_category_attributes_data = MarketCategoryAttribute.objects.filter(
        category__marketplace__marketplace=OZON,
    ).values(
        'id',
        'deleted',
        'required',
        'category_id',
        'is_collection',
        'attribute_id',
        'attribute__name',
        'attribute__source_id',
        'attribute__dictionary_id',
        'attribute__dictionary__source_id',
        'attribute__description',
        'attribute__data_type',
        'attribute__unit_id',
        'attribute__is_rich_content',
    )

    saved_attributes = {
        (
            values['attribute__source_id'],
            values['attribute__dictionary__source_id'] or 0,
        ):
            {
                'id': values['attribute_id'],
                'dictionary_id': values['attribute__dictionary_id'],
                'values': {
                    'name': values['attribute__name'],
                    'description': values['attribute__description'],
                    'data_type': values['attribute__data_type'],
                    'unit_id': values['attribute__unit_id'],
                    'is_rich_content': values['attribute__is_rich_content'],
                },
            }
        for values in saved_category_attributes_data
    }

    mapper_saved_attrs_log.info(saved_attributes)

    saved_category_attributes = {
        (
            values['category_id'],
            values['attribute_id'],
        ):
            {
                'id': values['id'],
                'values': {
                    'required': values['required'],
                    'is_collection': values['is_collection'],
                    'deleted': values['deleted'],
                },
            }
        for values in saved_category_attributes_data
    }

    saved_units = {
        unit_name.upper(): unit_id
        for unit_id, unit_name in
        ValueUnit.objects.values_list()
    }

    category_attribute_ids_to_delete = [
        values['id'] for values in saved_category_attributes_data
    ]

    fetched_attributes = attribute_data['attributes']

    mapper_fetched_attrs_log.info(fetched_attributes)

    for chunk in split_to_chunks(attribute_data['attributes'], 1000):
        with transaction.atomic():
            for attribute in chunk:
                source_id = attribute['id']
                dictionary_source_id = attribute['dictionary_id']

                attribute_key = (
                    source_id,
                    dictionary_source_id,
                )

                unit_id = None
                if (
                    not attribute['dictionary_id']
                    and attribute['type'] in [
                        'Decimal', 'Integer', 'String',
                    ]
                    and ',' in attribute['name']
                ):
                    probably_unit_name = attribute['name'].split(',')[-1]
                    unit_id = saved_units.get(
                        probably_unit_name.strip().upper(),
                    )

                attribute_values = {
                    'name': attribute['name'],
                    'description': attribute['description'],
                    'data_type': attribute['type'],
                    'unit_id': unit_id,
                    'is_rich_content': is_rich_content(attribute),
                }

                saved_attribute = saved_attributes.get(attribute_key)

                if not saved_attribute:
                    dictionary_id = saved_dictionaries[
                        dictionary_source_id
                    ] if dictionary_source_id else None
                    attribute_id = MarketAttribute.objects.create(
                        source_id=source_id,
                        dictionary_id=dictionary_id,
                        **attribute_values,
                    ).id
                    saved_attributes[attribute_key] = {
                        'id': attribute_id,
                        'dictionary_id': dictionary_id,
                        'values': attribute_values,
                    }
                else:
                    attribute_id = saved_attribute['id']
                    if attribute_values != saved_attribute['values']:
                        MarketAttribute.objects.filter(id=attribute_id).update(
                            **attribute_values,
                        )
                        saved_attribute['values'] = attribute_values

                category_id = categories[attribute['source_category_id']]
                category_attribute_key = (category_id, attribute_id)
                category_attribute_values = {
                    'required': attribute['is_required'],
                    'is_collection': attribute['is_collection'],
                    'deleted': False,
                }
                saved_category_attribute = saved_category_attributes.get(
                    category_attribute_key,
                )
                if not saved_category_attribute:
                    MarketCategoryAttribute.objects.create(
                        category_id=category_id,
                        attribute_id=attribute_id,
                        **category_attribute_values,
                    )
                else:
                    category_attribute_ids_to_delete.remove(
                        saved_category_attribute['id'],
                    )
                    if category_attribute_values != saved_category_attribute[
                        'values'
                    ]:
                        MarketCategoryAttribute.objects.filter(
                            id=saved_category_attribute['id'],
                        ).update(
                            **category_attribute_values,
                        )

    MarketCategoryAttribute.objects.filter(
        id__in=category_attribute_ids_to_delete,
    ).update(
        deleted=True,
    )

    MarketCategoryAttribute.objects.filter(
        attribute__source_id=str(YOUTUBE_CODE_ATTRIBUTE_ID),
        category__marketplace__marketplace=OZON,
    ).update(
        deleted=False,
    )

    MarketAttribute.objects.filter(
        source_id__in=MarketAttribute.SPECIAL_ATTRIBUTE_IDS.values()
    ).update(disabled=True)


def ozon_attributes_fetcher_main():
    """Ozon attributes fetcher main function."""
    attribute_data = get_attributes()

    populate_database(attribute_data)


if __name__ == '__main__':

    with run_checker('OzonAttributesFetcher'):
        setproctitle('OzonAttributesFetcher')
        ozon_attributes_fetcher_main()
