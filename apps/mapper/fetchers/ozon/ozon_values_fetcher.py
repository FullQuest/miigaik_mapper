"""Ozon attributes fetcher."""

import logging
import os
import sys

import django
from django.db import transaction
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.mapper.models import (  # noqa: e402
    MarketAttributeValue,
    CategoryMap,
    MarketCategoryAttribute,
    Marketplace,
)
from apps.mapper.utils.utils import (  # noqa: e402
    flatten,
)
from apps.ozon.utils.api_connector.seller.api_wrapper import (  # noqa: e402
    get_description_attribute_values,
)

from apps.utils.iterable_utils import split_to_chunks  # noqa: e402

from apps.utils import run_checker  # noqa: e402


log = logging.getLogger(__name__)

DOMAIN = 'www.b2basket.ru'
OZON = 'ozon'
BRAND_IDS = ['31', '85']
BLACK_LIST_DICTIONARY_IDS = [
    '9',  # люди и персонажи
    '4',  # серии
    '8',  # юрлица
    '7',  # издательства
]


def fetch_ozon_attribute_values(attribute_data):
    """Fetch values data from Ozon and prepare it."""

    category_id, type_id = attribute_data[
        'source_type_id'
    ].split('_')

    values_data = []

    has_next = True

    last_value_id = 0

    while has_next:
        values = get_description_attribute_values(
            domain=DOMAIN,
            category_id=category_id,
            type_id=type_id,
            attribute_id=attribute_data['source_id'],
            last_value_id=last_value_id,
            limit=5000,
        )
        if values and values['result']:
            has_next = values.get('has_next', True)
            last_value_id = values['result'][-1]['id']
            values_data.append(values['result'])
        else:
            has_next = False

    flat_values_data = flatten(values_data)

    for value_data in flat_values_data:
        value_data['picture_url'] = value_data.pop('picture')
        value_data['source_id'] = str(value_data.pop('id'))
        value_data['dictionary_id'] = attribute_data['dictionary_id']

    return flat_values_data


def populate_database(saved_values, values_data):
    """Populate values data into the database."""
    attribute_value_ids = []
    with transaction.atomic():
        for value_data in values_data:
            saved_value_data = saved_values.get(value_data['source_id'])

            if not saved_value_data:
                new_value_id = MarketAttributeValue.objects.create(
                    **value_data,
                ).id
                attribute_value_ids.append(new_value_id)

            else:
                value_values = {
                    k: v for k, v in value_data.items() if k in [
                        'value',
                        'picture_url',
                    ]
                }

                value_values['info'] = (
                    value_data.get('info', '')
                    or saved_value_data['values'].get('info', '')
                )

                value_values['deleted'] = False
                if value_values != saved_value_data['values']:
                    MarketAttributeValue.objects.filter(
                        id=saved_value_data['id'],
                    ).update(
                        **value_values,
                    )
                attribute_value_ids.append(saved_value_data['id'])

    return attribute_value_ids


def ozon_values_fetcher_main() -> bool:
    """Ozon values fetcher main function."""
    marketplace_id = Marketplace.objects.get(marketplace=OZON).pk
    mapped_categories_ids = CategoryMap.objects.filter(
        marketplace_category__marketplace_id=marketplace_id,
    ).values_list('marketplace_category_id', flat=True)

    attributes = {
        (
            category_attribute.category.source_id,
            category_attribute.attribute.dictionary_id,
        ) if category_attribute.attribute.source_id in BRAND_IDS else (
            0,
            category_attribute.attribute.dictionary_id,
        ):
        {
            'source_id': category_attribute.attribute.source_id,
            'source_type_id': category_attribute.category.source_id,  # NOTE: type_id contains category_id
            'dictionary_id': category_attribute.attribute.dictionary_id,
        }
        for category_attribute in MarketCategoryAttribute.objects.filter(
            deleted=False,
            category__marketplace__id=marketplace_id,
            category__deleted=False,
            attribute__dictionary__isnull=False,
            category_id__in=mapped_categories_ids,
        ).exclude(
            attribute__dictionary__source_id__in=BLACK_LIST_DICTIONARY_IDS,
        ).select_related('attribute', 'category')
    }

    for (_, dictionary_id), attribute in attributes.items():
        saved_values_data = MarketAttributeValue.objects.filter(
            dictionary_id=dictionary_id,
        ).values()

        saved_values = {
            value['source_id']: {
                'id': value['id'],
                'values': {
                    k: value[k] for k in [
                        'value',
                        'info',
                        'picture_url',
                        'deleted',
                    ]
                },
            }
            for value in saved_values_data
        }

        new_attribute_value_ids = []
        for chunk in split_to_chunks(
            fetch_ozon_attribute_values(attribute),
            10000,
        ):
            new_attribute_value_ids.extend(
                populate_database(saved_values, chunk),
            )

        if attribute['source_id'] in BRAND_IDS:
            continue

        values_to_delete_ids = list(
            {
                value['id']
                for value in saved_values_data
                if not value['deleted']
            } - set(new_attribute_value_ids),
        )
        for chunk in split_to_chunks(values_to_delete_ids, 1000):
            MarketAttributeValue.objects.filter(
                id__in=chunk,
            ).update(
                deleted=True,
            )

    print('DONE! •ᴗ•')

    return True


if __name__ == '__main__':

    with run_checker('OzonValuesFetcher'):
        setproctitle('OzonValuesFetcher')
        ozon_values_fetcher_main()
