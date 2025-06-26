"""Module with optimized queries for mapper."""

from typing import Dict, List, Any, Optional
from collections import (
    OrderedDict,
    defaultdict,
)

from apps.mapper.models import (
    FeedCategory,
    CategoryMap,
)

CategoryId = int
CategoryInfo = Dict[CategoryId, Dict[str, Any]]


def get_feed_category_tree_data(feed_id: int) -> List[Dict[str, Any]]:
    """Get category tree data for mapper feed view.

    Switched from serializer methods due to class @property methods.
    Some feeds have too many categories, and these recursive methods
    are very slow because each reference makes another call to MySQL.

    :param int feed_id: feed id in MySQL
    :return: category tree data
    :rtype: List[Dict[str, Any]]
    """
    categories = FeedCategory.objects.filter(feed=feed_id)

    if not categories:
        return []

    category_by_id = {
        category.id: OrderedDict([
            ('id', category.id),
            ('feed', category.feed_id),
            ('parent', category.parent_id),
            ('name', category.name),
            ('deleted', category.deleted),
            ('is_mapped', False),
            ('mapping_data', []),
            ('source_id', category.source_id),
            ('children', []),
        ])
        for category in categories
    }

    category_maps = CategoryMap.objects.filter(
        feed_category_id__in=category_by_id,
    ).values(
        'id',
        'feed_category__feed_id',
        'feed_category_id',
        'feed_category__name',
        'marketplace_category__marketplace_id',
        'marketplace_category__id',
        'marketplace_category__name',
        'marketplace_category__deleted',
    )

    for category_map in category_maps:
        mapping_data = {
            'mapping_id': category_map['id'],
            'feed_id': category_map['feed_category__feed_id'],
            'feed_category_id': category_map['feed_category_id'],
            'feed_category': category_map['feed_category__name'],
            'marketplace_id': category_map['marketplace_category__marketplace_id'],
            'marketplace_category_id': category_map['marketplace_category__id'],
            'marketplace_category': category_map['marketplace_category__name'],
            'marketplace_category_deleted': category_map['marketplace_category__deleted'],
        }

        category_by_id[category_map['feed_category_id']]['is_mapped'] = True
        category_by_id[category_map['feed_category_id']]['mapping_data'].append(mapping_data)

    source_category = [
        category for category in category_by_id.values()
        if category['source_id'] == -1
    ][0]

    if not source_category:
        return []

    categories_by_parent = defaultdict(list)
    for category_id, category_data in category_by_id.items():
        categories_by_parent[category_data['parent']].append(category_data)

    def _get_subcategories(
        parent_category_id: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Recursively get subcategories for category.

        :param Optional[int] parent_category_id: MySQL Category ID

        :return: category with subcategories
        :rtype: Dict[str, Any]
        """
        return [
            OrderedDict([   # NOTE: adding "children" attribute without losing an order
                *[(k, v) for k, v in category.items()],
                ('children', _get_subcategories(category['id']))
            ])
            for category in categories_by_parent[parent_category_id]
        ]

    return _get_subcategories()
