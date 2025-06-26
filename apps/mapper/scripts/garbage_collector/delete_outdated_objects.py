"""Module for deleting outdated objects, prepared before."""
import os
import sys
import django

from datetime import (
    datetime,
    timedelta,
)
from django.db.models import Count
from setproctitle import setproctitle
from typing import Optional

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.utils import run_checker  # noqa e402
from apps.mapper.models import (
    MarketCategory,
    MarketAttribute,
    MarketAttributeValue,
    MarketCategoryAttribute,
    MarketAttributeValueDictionary,
)
from apps.utils import MongoConnMixin
from apps.utils.iterable_utils import split_to_chunks
from apps.mapper.scripts.garbage_collector.prepare_objects_for_delete import (
    MONGO_MAPPER_DB,
    MP_CATEGORY_COLLECTION,
    MP_VALUES_COLLECTION,
    MP_DICTIONARIES_COLLECTION,
    MP_ATTRIBUTES_COLLECTION,
    MP_CATEGORY_ATTRIBUTE_COLLECTION,
)

DELETION_CHUNK_SIZE = 25000
SECONDS_OFFSET = 60 * 60 * 24 * 14  # 14 days


def delete_mp_attribute_values(deletion_threshold: datetime):
    conn = MongoConnMixin(MONGO_MAPPER_DB, MP_VALUES_COLLECTION)    # noqa
    deletion_ids = [
        obj['id'] for obj in conn.collection.find(
            {"timestamp": {"$lt": deletion_threshold}},
        )
    ]

    for ids_chunk in split_to_chunks(deletion_ids, DELETION_CHUNK_SIZE):
        MarketAttributeValue.objects.filter(
            pk__in=ids_chunk,
            deleted=True,
        ).delete()
        conn.collection.delete_many({'id': {'$in': ids_chunk}})


def delete_mp_dictionaries(deletion_threshold: datetime):
    conn = MongoConnMixin(MONGO_MAPPER_DB, MP_DICTIONARIES_COLLECTION)
    deletion_ids = [
        obj['id'] for obj in conn.collection.find(
            {"timestamp": {"$lt": deletion_threshold}},
        )
    ]
    for ids_chunk in split_to_chunks(deletion_ids, DELETION_CHUNK_SIZE):
        MarketAttributeValueDictionary.objects.annotate(
            values_count=Count('marketattributevalue'),
        ).filter(
            pk__in=ids_chunk,
            deleted=True,
            values_count=0,
        ).delete()

        conn.collection.delete_many({'id': {'$in': ids_chunk}})


def delete_mp_attributes(deletion_threshold: datetime):
    conn = MongoConnMixin(MONGO_MAPPER_DB, MP_ATTRIBUTES_COLLECTION)    # noqa
    deletion_ids = [
        obj['id'] for obj in conn.collection.find(
            {"timestamp": {"$lt": deletion_threshold}},
        )
    ]

    for ids_chunk in split_to_chunks(deletion_ids, DELETION_CHUNK_SIZE):
        MarketAttribute.objects.filter(
            pk__in=ids_chunk,
            deleted=True,
        ).delete()

        conn.collection.delete_many({'id': {'$in': ids_chunk}})


def delete_mp_category_attributes(deletion_threshold: datetime):
    conn = MongoConnMixin(MONGO_MAPPER_DB, MP_CATEGORY_ATTRIBUTE_COLLECTION)  # noqa
    deletion_ids = [
        obj['id'] for obj in conn.collection.find(
            {"timestamp": {"$lt": deletion_threshold}},
        )
    ]

    for ids_chunk in split_to_chunks(deletion_ids, DELETION_CHUNK_SIZE):
        MarketCategoryAttribute.objects.filter(
            pk__in=ids_chunk,
            deleted=True,
        ).delete()

        conn.collection.delete_many({'id': {'$in': ids_chunk}})


def delete_mp_category(deletion_threshold: datetime):
    conn = MongoConnMixin(MONGO_MAPPER_DB, MP_CATEGORY_COLLECTION)  # noqa
    deletion_ids = [
        obj['id'] for obj in conn.collection.find(
            {"timestamp": {"$lt": deletion_threshold}},
        )
    ]

    for ids_chunk in split_to_chunks(deletion_ids, DELETION_CHUNK_SIZE):
        MarketCategory.objects.filter(
            pk__in=ids_chunk,
            deleted=True,
        ).delete()

        conn.collection.delete_many({'id': {'$in': ids_chunk}})


def delete_prepared_objects(deletion_threshold: Optional[datetime] = None):
    """Delete all prepared objects from Mongo and MySQL

    All Mongo objects have same structure:
    {
        "id": %MYSQL_OBJECT_ID%,
        "timestamp": %ISO_TIME_WHEN_OBJECT_PREPARED_FOR_DELETION%,
    }

    For all object types:
    - Get Mongo objects that have deletion threshold exceeded
    - Delete MySQL objects that have same deletion condition as on
      preparation stage
    - Remove processed objects from Mongo

    Objects are being processed in this order:
    1. MarketAttributeValue
    2. MarketAttributeValueDictionary
    3. MarketCategoryAttribute
    4. MarketAttribute
    5. MarketCategory
    """
    deletion_threshold = (
        deletion_threshold
        or datetime.now() - timedelta(seconds=SECONDS_OFFSET)
    )

    delete_mp_attribute_values(deletion_threshold)
    delete_mp_dictionaries(deletion_threshold)
    delete_mp_category_attributes(deletion_threshold)
    delete_mp_attributes(deletion_threshold)
    delete_mp_category(deletion_threshold)


if __name__ == "__main__":
    with run_checker('MapperGarbageCollectorDeleteObjects', no_kill=True):
        setproctitle('MapperGarbageCollectorDeleteObjects')
        delete_prepared_objects()
