"""Test delete outdated objects."""
from datetime import timedelta
from unittest.mock import patch

from apps.mapper.models import (
    MarketAttributeValue,
    MarketCategory,
    MarketAttribute,
    MarketCategoryAttribute,
)
from apps.mapper.scripts.garbage_collector.delete_outdated_objects import (
    delete_mp_attribute_values,
    delete_mp_dictionaries,
    delete_mp_attributes,
    delete_mp_category_attributes,
    delete_mp_category, delete_prepared_objects,
)
from apps.mapper.scripts.garbage_collector.prepare_objects_for_delete import (
    prepare_mapper_objects_for_deletion,
    prepare_mp_values,
    prepare_mp_category,
)
from apps.mapper.tests.scripts.garbage_collector.garbage_collector_test_case import (
    MONGO_MAPPER_TEST_DB,
    GarbageCollectorTestCase,
)


class DeleteOutdatedObjectsTest(GarbageCollectorTestCase):

    def setUp(self):
        super(DeleteOutdatedObjectsTest, self).setUp()
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
        super(DeleteOutdatedObjectsTest, self).tearDown()
        self.drop_test_db()
        self.mongo_db_prepare_patch.stop()
        self.mongo_db_delete_patch.stop()

    def test_process_mp_attribute_values(self):
        cpu_dict = MarketAttribute.objects.get(name='cpu_manufacturer').dictionary

        cpu_manufacturers = MarketAttributeValue.objects.filter(dictionary=cpu_dict)
        cpu_manufacturers_ids = set(cpu_manufacturers.values_list('id', flat=True))
        cpu_manufacturers.update(deleted=True)

        prepare_mp_values(self.deletion_threshold)
        delete_mp_attribute_values(self.deletion_threshold+timedelta(hours=1))

        expected_differences = self.get_diff_template(
            removed={
                'mp_values': cpu_manufacturers_ids
            },
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    def test_delete_mp_dictionaries(self):
        cpu_attr = MarketAttribute.objects.get(name='cpu_manufacturer')
        cpu_attr_id = cpu_attr.id

        cpu_dict = cpu_attr.dictionary
        cpu_dict_id = cpu_dict.id

        cpu_category_attribute_id = MarketCategoryAttribute.objects.get(
            attribute_id=cpu_attr_id,
        ).id

        cpu_manufacturer_values = MarketAttributeValue.objects.filter(
            dictionary=cpu_dict,
        )
        cpu_manufacturer_values_ids = set(
            cpu_manufacturer_values.values_list('id', flat=True),
        )

        cpu_attr.delete()
        cpu_manufacturer_values.delete()

        # add dict to mongo; delete dict, that does not have values
        prepare_mapper_objects_for_deletion(self.deletion_threshold)
        delete_mp_dictionaries(self.deletion_threshold + timedelta(hours=1))

        expected_differences = self.get_diff_template(
            removed={
                'mp_values': cpu_manufacturer_values_ids,
                'mp_dictionaries': {cpu_dict_id},
                'mp_attributes': {cpu_attr_id},
                'mp_category_attributes': {cpu_category_attribute_id}
            },
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    def test_delete_mp_attributes(self):
        cpu_manufacturer_attr = MarketAttribute.objects.get(
            name='cpu_manufacturer')
        cpu_manufacturer_attr_id = cpu_manufacturer_attr.id

        cpu_manuf_dict_id = cpu_manufacturer_attr.dictionary_id

        cpu_cat_attr = MarketCategoryAttribute.objects.get(
            attribute_id=cpu_manufacturer_attr.id,
        )
        cpu_cat_attr_id = cpu_cat_attr.id
        cpu_cat_attr.delete()

        prepare_mapper_objects_for_deletion(self.deletion_threshold)
        delete_mp_attributes(self.deletion_threshold + timedelta(hours=1))

        expected_differences = self.get_diff_template(
            changed={
                'mp_dictionaries': {cpu_manuf_dict_id: {'deleted': True}},
                'mp_values': {
                    value.id: {'deleted': True} for value
                    in MarketAttributeValue.objects.filter(
                        dictionary_id=cpu_manuf_dict_id)
                }
            },
            removed={
                'mp_category_attributes': {cpu_cat_attr_id},
                'mp_attributes': {cpu_manufacturer_attr_id},
            }
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    def test_delete_mp_category_attributes(self):
        cpu_manufacturer = MarketAttribute.objects.get(name='cpu_manufacturer')
        self._set_deleted(cpu_manufacturer)

        gpu_category = MarketCategory.objects.get(name='gpu')
        self._set_deleted(gpu_category)

        removed_cat_attrs_ids = set(
            MarketCategoryAttribute.objects.filter(
                category_id=gpu_category.id,
            ).values_list('id', flat=True)
        )
        cpu_manufacturer_category_attribute_id = MarketCategoryAttribute.objects.get(
            attribute__name='cpu_manufacturer',
        ).id
        removed_cat_attrs_ids.add(cpu_manufacturer_category_attribute_id)

        cpu_manufacturer_dict_id = cpu_manufacturer.dictionary_id
        cpu_manufacturer_value_ids = MarketAttributeValue.objects.filter(
            dictionary_id=cpu_manufacturer_dict_id
        ).values_list('id', flat=True)

        gpu_manufacturer = MarketAttribute.objects.get(name='gpu_manufacturer')
        gpu_manufacturer_dict_id = gpu_manufacturer.dictionary_id
        gpu_manufacturer_value_ids = MarketAttributeValue.objects.filter(
            dictionary_id=gpu_manufacturer_dict_id
        ).values_list('id', flat=True)

        cpu_gpu_manufacture_value_ids = (
            list(cpu_manufacturer_value_ids) + list(gpu_manufacturer_value_ids)
        )

        prepare_mapper_objects_for_deletion(self.deletion_threshold)
        delete_mp_category_attributes(
            self.deletion_threshold+timedelta(hours=1))

        expected_differences = self.get_diff_template(
            removed={
                'mp_category_attributes': removed_cat_attrs_ids,
            },
            changed={
                'mp_categories':  {gpu_category.id: {'deleted': True}},
                'mp_attributes':  {
                    cpu_manufacturer.id: {'deleted': True},
                    gpu_manufacturer.id: {'deleted': True},
                },
                'mp_dictionaries': {
                    cpu_manufacturer_dict_id: {'deleted': True},
                    gpu_manufacturer_dict_id: {'deleted': True}
                },
                'mp_values': {
                    value_id: {'deleted': True}
                    for value_id in cpu_gpu_manufacture_value_ids
                },
            },
        )
        self.assertEqual(self.get_base_differences(), expected_differences)

    def test_delete_mp_category(self):
        gpu_category = MarketCategory.objects.get(name='gpu')
        gpu_category_id = gpu_category.id
        self._set_deleted(gpu_category)

        gpu_attributes = MarketCategoryAttribute.objects.filter(
            category_id=gpu_category.id,
        )
        gpu_attr_ids = set(gpu_attributes.values_list('id', flat=True))
        gpu_attributes.delete()

        prepare_mp_category(self.deletion_threshold)
        delete_mp_category(self.deletion_threshold+timedelta(hours=1))

        expected_differences = self.get_diff_template(
            removed={
                'mp_category_attributes': gpu_attr_ids,
                'mp_categories': {gpu_category_id}
            }
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    def test_delete_prepared_objects(self):
        delete_prepared_objects()

        # no changes
        self.assertEqual(self.get_base_differences(), self.get_diff_template())
