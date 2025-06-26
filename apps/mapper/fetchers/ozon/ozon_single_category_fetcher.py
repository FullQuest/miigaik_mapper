"""Module for fetching data for particular ozon category."""

from typing import Dict, Union
from django.db import transaction

from apps.mapper.models import (    # noqa: E402
    ValueUnit,
    CategoryMap,
    Marketplace,
    MarketCategory,
    MarketAttribute,
    MarketAttributeValue,
    MarketCategoryAttribute,
    MarketAttributeValueDictionary,
)
from apps.mapper.fetchers.ozon.ozon_attributes_fetcher import (  # noqa: E402
    fetch_ozon_category_attributes,
    is_rich_content,
    YOUTUBE_CODE_ATTRIBUTE_ID,
)
from apps.mapper.fetchers.ozon.ozon_values_fetcher import (  # noqa: E402
    OZON,
    BRAND_IDS,
    BLACK_LIST_DICTIONARY_IDS,
    populate_database as populate_db_values,
    fetch_ozon_attribute_values,
)
from apps.utils.iterable_utils import split_to_chunks  # noqa: E402


def update_ozon_category(source_id: str):
    """Update full category attributes and attribute values."""
    attributes_data = fetch_category_attributes(source_id)

    populate_db_attributes(
        attributes_data,
        source_id,
    )

    process_attribute_values(source_id)


def fetch_category_attributes(source_id: str):
    """Fetch attributes for given category."""
    attributes = fetch_ozon_category_attributes([source_id])

    attribute_data: Dict[str, Union[set, list]] = {
        'dictionary_ids': {
            attribute['dictionary_id']
            for attribute
            in attributes
        },
        'attributes': attributes,
    }

    return attribute_data


def process_attribute_values(source_id: str):
    """Fetch ozon attribute values."""
    marketplace_id = Marketplace.objects.get(marketplace=OZON).pk
    mapped_categories_ids = CategoryMap.objects.filter(
        marketplace_category__marketplace_id=marketplace_id,
        marketplace_category__source_id=source_id,
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
                populate_db_values(saved_values, chunk),
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
        for chunk in split_to_chunks(values_to_delete_ids, 100):
            try:
                MarketAttributeValue.objects.filter(
                    id__in=chunk,
                ).update(
                    deleted=True,
                )
            except Exception:
                raise

    print('DONE! •ᴗ•')


def populate_db_attributes(
    attribute_data,
    source_id: str,
):
    """Populate attribute data into the database."""
    fetched_dictionary_ids = set(filter(
        bool,
        attribute_data['dictionary_ids'],
    ))

    saved_dictionaries = {
        values['attribute__dictionary__source_id']:
            values['attribute__dictionary_id']
        for values in MarketCategoryAttribute.objects.filter(
            category__marketplace__marketplace=OZON,
            category__source_id=source_id,
        ).values(
            'attribute__dictionary_id',
            'attribute__dictionary__source_id',
        )
    }

    saved_dictionary_ids = set(saved_dictionaries)

    new_dictionary_ids = list(fetched_dictionary_ids - saved_dictionary_ids)

    for chunk in split_to_chunks(
        new_dictionary_ids,
        5000,
    ):
        with transaction.atomic():
            for dictionary_source_id in chunk:
                dictionary_id = \
                    MarketAttributeValueDictionary.objects.create(
                        source_id=dictionary_source_id,
                    ).id  # type: ignore
                saved_dictionaries[dictionary_source_id] = dictionary_id

    categories = {
        values['source_id']: values['id']
        for values in MarketCategory.objects.filter(
            marketplace__marketplace=OZON,
            source_id=source_id,
        ).values('id', 'source_id')
    }

    saved_category_attributes_data = MarketCategoryAttribute.objects.filter(
        category__marketplace__marketplace=OZON,
        category__source_id=source_id,
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

                    # Changed from create to get_or_create for those situations
                    # when category does not contain MarketAttr in it in
                    # our DB, but if it fetches and contain it we need to
                    # connect it with already existing one and not create a new
                    try:
                        attribute_id = MarketAttribute.objects.get_or_create(
                            source_id=source_id,
                            dictionary_id=dictionary_id,
                            **attribute_values,
                        )[0].id  # type: ignore
                    except MarketAttribute.MultipleObjectsReturned:
                        attribute_id = MarketAttribute.objects.filter(
                            source_id=source_id,
                            dictionary_id=dictionary_id,
                            **attribute_values,
                        )[0].id

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

    for delete_attr in category_attribute_ids_to_delete:
        MarketCategoryAttribute.objects.filter(
            id=delete_attr,
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