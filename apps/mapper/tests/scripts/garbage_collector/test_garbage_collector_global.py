from apps.mapper.tests.scripts.garbage_collector.garbage_collector_test_case import (
    GarbageCollectorTestCase,
    MONGO_MAPPER_TEST_DB,
)
from apps.mapper.scripts.garbage_collector.prepare_objects_for_delete import (
    prepare_mapper_objects_for_deletion,
)
from apps.mapper.scripts.garbage_collector.delete_outdated_objects import (
    delete_prepared_objects,
)
from apps.mapper.models import (
    MarketAttributeValue,
    MarketCategoryAttribute,
    MarketAttribute,
    MarketCategory,
)
from unittest.mock import patch
from datetime import timedelta


class GarbageCollectorGlobalTest(GarbageCollectorTestCase):

    def setUp(self):
        super(GarbageCollectorGlobalTest, self).setUp()
        self.mongo_db_prepare_patch = patch(
            'apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.MONGO_MAPPER_DB',
            new=MONGO_MAPPER_TEST_DB,
        )
        self.mongo_db_delete_patch = patch(
            'apps.mapper.scripts.garbage_collector.delete_outdated_objects.MONGO_MAPPER_DB',
            new=MONGO_MAPPER_TEST_DB,
        )
        self.mongo_db_prepare_patch.start()
        self.mongo_db_delete_patch.start()

    def tearDown(self):
        super(GarbageCollectorGlobalTest, self).tearDown()
        self.drop_test_db()
        self.mongo_db_prepare_patch.stop()
        self.mongo_db_delete_patch.stop()

    def run_collector(self, prepare_delta_secs: int = -1):
        prepare_mapper_objects_for_deletion(self.deletion_threshold + timedelta(seconds=prepare_delta_secs))
        delete_prepared_objects(self.deletion_threshold)

    def test_cascade_category_deletion(self):
        cpu_category = MarketCategory.objects.get(name='cpu')
        cpu_category_id = cpu_category.id
        self._set_deleted(cpu_category)

        deleted_cpu_cat_attr_ids = set(
            MarketCategoryAttribute.objects.filter(
                category__name='cpu',
            ).values_list('id', flat=True)
        )

        deleted_cpu_manuf_attr = MarketAttribute.objects.get(name='cpu_manufacturer')
        deleted_cpu_manuf_attr_id = deleted_cpu_manuf_attr.id

        deleted_cpu_manuf_dictionary_id = deleted_cpu_manuf_attr.dictionary_id

        deleted_cpu_manuf_value_ids = set(
            MarketAttributeValue.objects.filter(
                dictionary_id=deleted_cpu_manuf_dictionary_id
            ).values_list('id', flat=True)
        )

        self.run_collector()

        expected_changes = self.get_diff_template(
            removed={
                'mp_categories': {cpu_category_id},
                'mp_category_attributes': deleted_cpu_cat_attr_ids,
                'mp_attributes': {deleted_cpu_manuf_attr_id},
                'mp_dictionaries': {deleted_cpu_manuf_dictionary_id},
                'mp_values': deleted_cpu_manuf_value_ids,
            }
        )

        self.assertEqual(expected_changes, self.get_base_differences())

    def test_cascade_restored(self):
        cpu_category = MarketCategory.objects.get(name='cpu')
        self._set_deleted(cpu_category)

        prepare_mapper_objects_for_deletion(self.deletion_threshold)

        self._set_deleted(cpu_category, False)
        MarketCategoryAttribute.objects.filter(
            category__name='cpu'
        ).update(deleted=False)

        market_attribute = MarketAttribute.objects.get(name='cpu_manufacturer')
        self._set_deleted(market_attribute, False)

        cpu_manuf_dict = market_attribute.dictionary
        self._set_deleted(cpu_manuf_dict, False)

        MarketAttributeValue.objects.filter(
            dictionary_id=cpu_manuf_dict.id,
        ).update(deleted=False)

        delete_prepared_objects()

        # no changes
        self.assertEqual(self.get_diff_template(), self.get_base_differences())

    def test_no_changes(self):
        self.run_collector()
        self.assertEqual(self.get_diff_template(), self.get_base_differences())
        self.run_collector()
        self.assertEqual(self.get_diff_template(), self.get_base_differences())
