"""Module is part of Garbage Collector.

This script:
1. Puts objects ready to be deleted to mongo.
2. Marks as deleted those objects that deleted by MP or don't
    have any relation.

Notes:
    - This script does not delete anything! Only prepares objects for deletion
    - All related collections are in mongo db named "mapper"
"""
import os
import sys
import django

from django.db import connection
from datetime import datetime
from django.db.models import Count, QuerySet, Q
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.utils import run_checker                      # noqa e402
from apps.utils.iterable_utils import split_to_chunks   # noqa
from apps.utils import MongoConnMixin                   # noqa
from apps.mapper.models import (                        # noqa
    MarketAttributeValueDictionary,
    MarketCategoryAttribute,
    MarketAttributeValue,
    MarketAttribute,
    MarketCategory,
)

BASE_CHUNK_SIZE = 2000
MONGO_MAPPER_DB = 'mapper'

MP_VALUES_COLLECTION = "mp_sched_delete_attribute_value"                    # For MarketAttributeValue
MP_DICTIONARIES_COLLECTION = "mp_sched_delete_attribute_value_dict"         # For MarketAttributeValueDictionary
MP_ATTRIBUTES_COLLECTION = "mp_sched_delete_attribute"                      # For MarketAttribute
MP_CATEGORY_ATTRIBUTE_COLLECTION = "mp_sched_delete_category_attribute"     # For MarketCategoryAttribute
MP_CATEGORY_COLLECTION = "mp_sched_delete_category"                         # For MarketCategory


def get_existing_mongo_ids(
    object_ids,
    conn: MongoConnMixin,
    batch_size=100000
):
    existing_ids = set()

    for i in range(0, len(object_ids), batch_size):
        batch = object_ids[i:i + batch_size]

        existing_ids.update(
            doc['id'] for doc in conn.collection.find(
                {'id': {'$in': batch}},
                {'id': 1, '_id': 0},
            )
        )

    return existing_ids


def mongo_garbage_insert(
    objects: QuerySet,
    date_prepared: datetime,
    collection_name: str,
    db_name: str = None
):
    conn = MongoConnMixin(db_name or MONGO_MAPPER_DB, collection_name)

    value_ids = list(objects.values_list('id', flat=True))

    already_in_mongo_ids = get_existing_mongo_ids(value_ids, conn)

    deletion_objects = [
        {
            "id": value_id,
            "timestamp": date_prepared,
        }
        for value_id in value_ids
        if value_id not in already_in_mongo_ids
    ]

    for chunk in split_to_chunks(deletion_objects, BASE_CHUNK_SIZE):
        conn.collection.insert_many(chunk)


def prepare_mp_values(
    date_prepared: datetime,
    collection: str = MP_VALUES_COLLECTION,
):
    """Prepare marketplace values.

    Schedule `MarketAttributeValue` for delete:

    1. if it associated with:
        `MarketAttributeValueDictionary.deleted=True`
    2. if `MarketAttributeValue.deleted=True`
    """
    with connection.cursor() as cursor:
        cursor.execute("""
            UPDATE mapper_marketattributevalue
            SET deleted = TRUE
            WHERE deleted = FALSE
            AND dictionary_id IN (
                SELECT id
                FROM mapper_marketattributevaluedictionary
                WHERE deleted = TRUE
            )
        """)

    mongo_garbage_insert(
        MarketAttributeValue.objects.filter(deleted=True),
        date_prepared,
        collection,
    )


def prepare_mp_dictionaries(
    date_prepared: datetime,
    collection: str = MP_DICTIONARIES_COLLECTION,
):
    """Prepare marketplace dictionaries.

    Schedule `MarketAttributeValueDictionary` for delete:

    1. if it associated only with:
        `MarketAttribute.deleted=True`
    2. if `MarketAttributeValueDictionary.deleted=True`
    """
    MarketAttributeValueDictionary.objects.annotate(
        attributes_count=Count(
            'marketattribute',
            filter=Q(marketattribute__deleted=False),
        )
    ).filter(
        attributes_count=0,
        deleted=False,
    ).update(deleted=True)

    mongo_garbage_insert(
        MarketAttributeValueDictionary.objects.filter(deleted=True),
        date_prepared,
        collection,
    )


def prepare_mp_attributes(
    date_prepared: datetime,
    collection: str = MP_ATTRIBUTES_COLLECTION,
):
    """Prepare marketplace attributes.

    Schedule `MarketAttribute` for delete:

    1. if it associated only with:
        `MarketCategoryAttribute.deleted=True`
    2. if `MarketAttribute.deleted=True`
    """
    MarketAttribute.objects.annotate(
        attributes_count=Count(
            'marketcategoryattribute',
            filter=Q(marketcategoryattribute__deleted=False),
        )
    ).filter(
        attributes_count=0,
        deleted=False,
    ).update(deleted=True)

    mongo_garbage_insert(
        MarketAttribute.objects.filter(deleted=True),
        date_prepared,
        collection,
    )


def prepare_mp_category_attributes(
    date_prepared: datetime,
    collection: str = MP_CATEGORY_ATTRIBUTE_COLLECTION,
):
    """Prepare marketplace category attribute.

    Schedule `MarketCategoryAttribute` for delete:

    1. if it associated with:
        `MarketCategory.deleted=True` OR `MarketAttribute.deleted=True`
    2. if `MarketCategoryAttribute.deleted=True`
    """
    MarketCategoryAttribute.objects.filter(
        Q(category__deleted=True) | Q(attribute__deleted=True),
        deleted=False
    ).update(deleted=True)

    mongo_garbage_insert(
        MarketCategoryAttribute.objects.filter(deleted=True),
        date_prepared,
        collection,
    )


def prepare_mp_category(
    date_prepared: datetime,
    collection: str = MP_CATEGORY_COLLECTION,
):
    """Prepare marketplace category.

    Schedule `MarketCategoryAttribute` for delete:

    1. if `MarketCategory.deleted=True`
    """
    mongo_garbage_insert(
        MarketCategory.objects.filter(deleted=True),
        date_prepared,
        collection,
    )


def prepare_mapper_objects_for_deletion(prepared_date: datetime = None):
    """Algorythm:

    1. Process MarketCategory
        add to mongo MarketCategory that:
            `MarketCategory.deleted==True`,

    2. Process MarketCategoryAttribute
        set `MarketCategoryAttribute.deleted=True` for objects that
                related to `MarketCategory.deleted==True`
                        OR `MarketAttribute.deleted==True`
            AND `MarketCategoryAttribute.deleted==False`
        add to mongo `MarketCategoryAttribute.deleted==True`

    3. Process MarketAttribute
        set `MarketAttribute.deleted=True` for objects that
                not related with any `MarketCategoryAttribute.deleted=False`
        add to mongo `MarketAttribute.deleted=True`

    4. Process MarketAttributeValueDictionary
        set `MarketAttributeValueDictionary.deleted=True` for objects that
            not related with any MarketAttribute
        add to mongo MarketAttributeValueDictionary that:
                not related with any MarketAttributeValue
            AND not related with any MarketAttribute
            AND `MarketAttributeValueDictionary.deleted==True`

    5. Process MarketAttributeValue:
        set `MarketAttributeValue.deleted=True` for objects that
                referred to `MarketAttributeValueDictionary.deleted=True`
        add `MarketAttributeValue.deleted==True` to mongo

    """
    prepared_date = prepared_date or datetime.now()

    prepare_mp_category(prepared_date)
    prepare_mp_category_attributes(prepared_date)
    prepare_mp_attributes(prepared_date)
    prepare_mp_dictionaries(prepared_date)
    prepare_mp_values(prepared_date)


if __name__ == "__main__":
    with run_checker('MapperGarbageCollectorPrepareObjects', no_kill=True):
        setproctitle('MapperGarbageCollectorPrepareObjects')
        prepare_mapper_objects_for_deletion()
