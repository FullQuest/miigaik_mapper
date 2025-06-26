"""Populate mysql with feed categories."""

import logging
import os
import sys
from multiprocessing import Pool
from typing import Dict, List, Union
from pymongo.errors import AutoReconnect

import django
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.mapper.models import FeedCategory, FeedMeta
from apps.mapper.utils.utils import (
    flatten,
    get_feed_ids,
)
from apps.utils import MongoConnMixin, run_checker
from apps.utils.retry_utils import retry_on


log = logging.getLogger(__name__)

CategoryData = Dict[str, Union[int, str]]
CategoriesData = List[Dict[str, Union[int, str]]]


@retry_on(AutoReconnect)
def fetch_feed_categories(feed_id: int) -> CategoriesData:
    """Fetch all feed categories from parsed feed.

    :param int feed_id: Feed id

    :return categories_data: List of dict repr of categories from Mongo
    :rtype: CategoriesData
    """
    collection = f'feed_{feed_id}'

    conn = MongoConnMixin(
        dbname='mapper',
        collection=collection,
        subcollection='categories',
    )

    categories = conn.collection.find({}, {'_id': False})

    categories_data = [
        {
            'feed_id': feed_id,
            'category_id': c['@id'],
            'parent_id': c.get('@parentId') and int(c.get('@parentId')),
            'name': c['#text'],
        }
        for c in categories
    ]

    return categories_data


def populate_database(
    category_data: CategoryData,
    deleted: bool = False,
):
    """Update or create FeedCategory object, set "deleted" flag if required.

    :param category_data: Dict representation of category
    :type category_data: CategoryData

    :param bool deleted: True if category needs to be flagged as deleted

    :return category: Created or updated category object
    :rtype: FeedCategory
    """
    feed = FeedMeta.objects.get(id=category_data['feed_id'])

    data = {
        'feed': feed,
        'source_id': category_data['category_id'],
        'defaults': {
            'deleted': deleted,
            'name': category_data['name'],
        },
    }

    category, _ = FeedCategory.objects.update_or_create(**data)

    save_data = {
        'id': category.id,
        'parent_id': category_data['parent_id'] or -1,
        'feed_id': category_data['feed_id'],
    }

    return save_data


def set_parents(category_data, main_categories):
    """Set category parent_id."""
    if category_data['parent_id']:
        feed_id = category_data['feed_id']
        try:
            parent = FeedCategory.objects.get(
                source_id=category_data['parent_id'],
                feed_id=feed_id,
            )
        except FeedCategory.DoesNotExist:
            parent = FeedCategory.objects.get(
                id=main_categories[feed_id],
                feed_id=feed_id,
            )

        FeedCategory.objects.filter(
            id=category_data['id']
        ).update(parent=parent)


def set_deleted():
    pass


def set_restored():
    pass


def get_main_categories(feed_ids):
    """Get main categories for every feed."""
    return {
        feed_id: FeedCategory.objects.get_or_create(
            feed_id=feed_id,
            source_id=-1,
            name='Каталог',
        )[0].id
        for feed_id in feed_ids
    }


def feed_categories_fetcher_main():
    """Fetch feed categories data from MongoDB and populate MySQL.

    Sets flag "deleted" to category if it was removed from feed,
    and restores it, if it was added again.
    """
    feed_ids = get_feed_ids()

    with Pool(processes=5) as pool:
        categories_data: List[CategoriesData] = pool.map(
            fetch_feed_categories,
            feed_ids,
        )

    mongo_categories: CategoriesData = flatten(categories_data)

    population_result = []

    for category in mongo_categories:
        population_result.append(populate_database(category))

    main_categories = get_main_categories(feed_ids)

    for category_data in population_result:
        set_parents(category_data, main_categories)


if __name__ == '__main__':

    with run_checker('MapperFeedCategoriesFetcher'):
        setproctitle('MapperFeedCategoriesFetcher')
        feed_categories_fetcher_main()
