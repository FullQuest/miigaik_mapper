"""Ozon categories fetcher."""

import logging
import os
import sys
from typing import Any, Dict, Generator, List

import django
from django.db import transaction
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.mapper.models import (
    MarketCategory,
    Marketplace,
)
from apps.mapper.utils.utils import (
    create_market_category_dict,
)
from apps.ozon.utils.api_connector.seller.api_wrapper import (
    get_description_category_tree,
)
from apps.utils import run_checker


log = logging.getLogger(__name__)

DOMAIN = 'www.b2basket.ru'
OZON = 'ozon'

Category = Dict[str, Any]
Categories = List[Category]
CategoryTree = List[Dict[str, Any]]
CategoryGenerator = Generator[Dict[str, Any], None, None]


def fetch_ozon_categories() -> CategoryTree:
    """Fetch categories from Ozon.

    :return category_tree: Category tree as dict
    :rtype: CategoryTree
    """
    category_tree = get_description_category_tree(
        domain=DOMAIN,
        language=None,
        trace_requests=True,
    )

    return category_tree


def category_generator(category_tree: CategoryTree) -> CategoryGenerator:
    """FIXME."""
    category_list = list(category_tree)

    for top in category_list:
        yield create_market_category_dict(
            marketplace=OZON,
            source_id=top['description_category_id'],
            disabled=top['disabled'],
            parent_id=None,
            name=top['category_name'],
            leaf=False,
        )
        for sub in top['children']:
            yield create_market_category_dict(
                marketplace=OZON,
                source_id=sub['description_category_id'],
                disabled=sub['disabled'],
                parent_id=top['description_category_id'],
                name=sub['category_name'],
                leaf=False,
            )
            for child in sub['children']:
                yield create_market_category_dict(
                    marketplace=OZON,
                    # NOTE: last category (type) saved with prefix to prevent duplicates
                    source_id=f'{sub["description_category_id"]}_{child["type_id"]}',
                    disabled=child['disabled'],
                    parent_id=sub['description_category_id'],
                    name=child['type_name'],
                    leaf=True,
                )


def get_categories(category_tree: CategoryTree) -> Categories:
    """Unwrap categories"""
    categories = []
    gen = category_generator(category_tree)

    for category in gen:
        categories.append(category)

    return categories


def populate_database(category_data: Category):
    """Populate db category"""
    marketplace = Marketplace.objects.get(marketplace=OZON)

    data = {
        'marketplace': marketplace,
        'source_id': category_data['source_id'],
        'defaults': {
            'name': category_data['name'],
            'leaf': category_data['leaf'],
            'deleted': category_data['disabled'],
        },
    }

    category, _ = MarketCategory.objects.update_or_create(**data)

    save_data = {
        'id': category.id,
        'parent_id': category_data['parent_id'],
        'marketplace_id': marketplace.id,
    }

    return save_data


def set_parents(category_data):
    """Set category parent_id."""
    if category_data['parent_id']:
        parent = MarketCategory.objects.get(
            source_id=category_data['parent_id'],
            marketplace_id=category_data['marketplace_id'],
        )

        MarketCategory.objects.filter(
            id=category_data['id']).update(parent=parent)


def set_deleted_categories(categories):
    """Set old categories as deleted."""
    MarketCategory.objects.filter(
        deleted=False,
    ).exclude(
        id__in=[category['id'] for category in categories],
    ).update(deleted=True)


def ozon_categories_fetcher_main():
    """
    1. Fetch category tree
    2. Flatten graph categories tree into list
    3. Populate db
    4. Create parent connections
    5. Set deleted categories
    """
    category_tree = fetch_ozon_categories()

    categories = get_categories(category_tree)

    population_result = []

    with transaction.atomic():
        for category in categories:
            population_result.append(populate_database(category))

        for result in population_result:
            set_parents(result)

        set_deleted_categories(population_result)


if __name__ == '__main__':

    with run_checker('OzonCategoriesFetcher'):
        setproctitle('OzonCategoriesFetcher')
        ozon_categories_fetcher_main()
