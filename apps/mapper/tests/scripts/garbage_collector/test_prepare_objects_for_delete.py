"""Test prepare objects for delete."""
from unittest.mock import (
    patch,
    MagicMock,
)

from apps.mapper.models import (
    MarketAttributeValueDictionary,
    MarketAttributeValue,
    MarketCategory,
    MarketAttribute,
    MarketCategoryAttribute,
)
from apps.mapper.scripts.garbage_collector.prepare_objects_for_delete import (
    MP_VALUES_COLLECTION,
    MP_DICTIONARIES_COLLECTION,
    MP_ATTRIBUTES_COLLECTION,
    MP_CATEGORY_ATTRIBUTE_COLLECTION,
    MP_CATEGORY_COLLECTION,
    mongo_garbage_insert,
    prepare_mp_values,
    prepare_mp_dictionaries,
    prepare_mp_attributes,
    prepare_mp_category_attributes,
    prepare_mp_category,
    prepare_mapper_objects_for_deletion,
)
from apps.mapper.tests.scripts.garbage_collector.garbage_collector_test_case import (
    GarbageCollectorTestCase,
)


class PrepareObjectsForDeleteTest(GarbageCollectorTestCase):

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.MongoConnMixin')
    def test_mongo_garbage_insert(self, mock_mongo_conn):
        mock_market_value_dict = MarketAttributeValueDictionary(
            id=123,
            source_id=123,
        )
        mock_market_value_dict.save()

        MarketAttributeValue(id=1, dictionary=mock_market_value_dict).save()
        MarketAttributeValue(id=2, dictionary=mock_market_value_dict).save()
        MarketAttributeValue(id=3, dictionary=mock_market_value_dict).save()

        deletion_objects = MarketAttributeValue.objects.filter(
            dictionary=mock_market_value_dict,
        )
        deletion_objects_ids = list(
            deletion_objects.values_list('id', flat=True))

        collection_name = 'test_collection'

        mock_conn_instance = MagicMock()
        mock_mongo_conn.return_value = mock_conn_instance
        mock_conn_instance.collection.find.return_value = [{'id': 1}]

        mongo_garbage_insert(
            deletion_objects,
            self.deletion_threshold,
            collection_name,
        )

        mock_conn_instance.collection.find.assert_called_once_with(
            {
                'id': {
                    '$in': deletion_objects_ids,
                },
            },
            {'id': 1, '_id': 0},
        )

        expected_chunk = [
            {"id": 2, "timestamp": self.deletion_threshold},
            {"id": 3, "timestamp": self.deletion_threshold},
        ]
        mock_conn_instance.collection.insert_many.assert_called_once_with(
            expected_chunk,
        )

        mock_market_value_dict.delete()

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_process_values(self, mock_mongo_garbage_insert):
        cpu_manuf_dict = MarketAttributeValueDictionary.objects.get(id=1)
        self._set_deleted(cpu_manuf_dict)

        cpu_values = MarketAttributeValue.objects.filter(dictionary=cpu_manuf_dict)
        self._set_deleted(cpu_values[0])

        prepare_mp_values(self.deletion_threshold)

        values_to_delete = MarketAttributeValue.objects.filter(
            deleted=True,
        )

        mock_mongo_garbage_insert.called_once_with(
            values_to_delete,
            self.deletion_threshold,
            MP_VALUES_COLLECTION,
        )

        expected_differences = self.get_diff_template(
            changed={
                'mp_dictionaries': {cpu_manuf_dict.id: {'deleted': True}},
                'mp_values': {
                    value.id: {'deleted': True}
                    for value in cpu_values
                }
            }
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_process_dictionaries(self, mock_mongo_garbage_insert):
        gpu_manuf_attr = MarketAttribute.objects.get(name='gpu_manufacturer')
        manuf_attributes_ids = {gpu_manuf_attr.id}

        gpu_manuf_values = MarketAttributeValue.objects.filter(
            dictionary_id=gpu_manuf_attr.dictionary_id,
        )
        gpu_manuf_values_ids = set(gpu_manuf_values.values_list('id', flat=True))

        gpu_manuf_dict = gpu_manuf_attr.dictionary
        gpu_manuf_dict.deleted = True
        gpu_manuf_dict.save()

        cpu_manuf_attr = MarketAttribute.objects.get(name='cpu_manufacturer')
        manuf_attributes_ids.add(cpu_manuf_attr.id)

        cpu_manuf_dict = cpu_manuf_attr.dictionary

        manuf_cat_attr_ids = set(MarketCategoryAttribute.objects.filter(
            attribute_id__in=manuf_attributes_ids
        ).values_list('id', flat=True))

        gpu_manuf_values.delete()
        gpu_manuf_attr.delete()
        cpu_manuf_attr.delete()

        prepare_mp_dictionaries(self.deletion_threshold)

        mock_mongo_garbage_insert.called_once_with(
            MarketAttributeValueDictionary.objects.filter(
                id=gpu_manuf_dict.id
            ),  # filter instead of get to make queryset instead of instance
            self.deletion_threshold,
            MP_DICTIONARIES_COLLECTION,
        )

        expected_differences = self.get_diff_template(
            changed={
                'mp_dictionaries': {
                    cpu_manuf_dict.id: {'deleted': True},
                    gpu_manuf_dict.id: {'deleted': True}
                }
            },
            removed={
                'mp_attributes': manuf_attributes_ids,
                'mp_category_attributes': manuf_cat_attr_ids,
                'mp_values': gpu_manuf_values_ids,
            }
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_process_attributes(self, mock_mongo_garbage_insert):
        gpu_attr = MarketAttribute.objects.get(name='gpu_manufacturer')
        gpu_cat_attr = MarketCategoryAttribute.objects.get(attribute_id=gpu_attr.id)

        manuf_cat_attrs = {gpu_cat_attr.id}

        gpu_cat_attr.delete()
        self._set_deleted(gpu_attr)

        cpu_attr = MarketAttribute.objects.get(name='cpu_manufacturer')
        cpu_cat_attr = MarketCategoryAttribute.objects.get(attribute_id=cpu_attr.id)

        manuf_cat_attrs.add(cpu_cat_attr.id)
        cpu_cat_attr.delete()

        prepare_mp_attributes(self.deletion_threshold)

        mock_mongo_garbage_insert.called_once_with(
            MarketAttribute.objects.filter(
                id=gpu_attr.id
            ),  # filter instead of get to make queryset instead of instance
            self.deletion_threshold,
            MP_ATTRIBUTES_COLLECTION,
        )

        expected_differences = self.get_diff_template(
            changed={
                'mp_attributes': {
                    gpu_attr.id: {'deleted': True},
                    cpu_attr.id: {'deleted': True}
                }
            },
            removed={'mp_category_attributes': manuf_cat_attrs}
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_process_market_category_attribute(self, mock_mongo_garbage_insert):
        cpu_category = MarketCategory.objects.get(name='cpu')
        self._set_deleted(cpu_category)

        gpu_cat_attr = MarketCategoryAttribute.objects.get(attribute__name='gpu_manufacturer')
        self._set_deleted(gpu_cat_attr)

        prepare_mp_category_attributes(self.deletion_threshold)

        mock_mongo_garbage_insert.called_once_with(
            MarketCategoryAttribute.objects.filter(
                id=gpu_cat_attr.id
            ),  # filter instead of get to make queryset instead of instance
            self.deletion_threshold,
            MP_CATEGORY_ATTRIBUTE_COLLECTION,
        )

        cpu_attributes_mark_deleted = MarketCategoryAttribute.objects.filter(
            category=cpu_category
        )

        expected_differences = self.get_diff_template(
            changed={
                'mp_category_attributes': {
                    gpu_cat_attr.id: {'deleted': True},
                    **{
                        cat_attr.id: {'deleted': True}
                        for cat_attr in cpu_attributes_mark_deleted
                    }
                },
                'mp_categories': {
                    cpu_category.id: {'deleted': True},
                },
            }
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_process_category(self, mock_mongo_garbage_insert):
        cpu_attributes = MarketCategoryAttribute.objects.filter(
            category=MarketCategory.objects.get(name='cpu')
        )
        cpu_attributes_ids = set(cpu_attributes.values_list('id', flat=True))
        cpu_attributes.delete()

        gpu_category = MarketCategory.objects.get(name='gpu')
        gpu_attributes = MarketCategoryAttribute.objects.filter(
            category=gpu_category
        )
        gpu_attributes_ids = set(gpu_attributes.values_list('id', flat=True))
        gpu_attributes.delete()
        self._set_deleted(gpu_category)

        prepare_mp_category(self.deletion_threshold)

        mock_mongo_garbage_insert.called_once_with(
            MarketCategory.objects.filter(
                id=gpu_category.id
            ),  # filter instead of get to make queryset instead of instance
            self.deletion_threshold,
            MP_CATEGORY_COLLECTION,
        )

        expected_differences = self.get_diff_template(
            changed={
                'mp_categories': {
                    gpu_category.id: {'deleted': True},
                },
            },
            removed={
                'mp_category_attributes': (
                    cpu_attributes_ids | gpu_attributes_ids
                ),
            },
        )

        self.assertEqual(self.get_base_differences(), expected_differences)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_prepare_mapper_objects_for_deletion_category(self, _mock_mongo_garbage_insert):
        cpu_category_attributes = MarketCategoryAttribute.objects.filter(
            category__name='cpu'
        )
        cpu_attribute = MarketAttribute.objects.get(name='cpu_manufacturer')

        cpu_category = MarketCategory.objects.get(name='cpu')
        cpu_category_id = cpu_category.id
        self._set_deleted(cpu_category)

        prepare_mapper_objects_for_deletion()

        expected_diffs = self.get_diff_template(
            changed={
                'mp_categories': {
                    cpu_category_id: {'deleted': True}
                },
                'mp_category_attributes': {
                    attr.id: {'deleted': True}
                    for attr in cpu_category_attributes
                },
                'mp_attributes': {
                    cpu_attribute.id: {'deleted': True},
                },
                'mp_dictionaries': {
                    cpu_attribute.dictionary_id: {'deleted': True},
                },
                'mp_values': {
                    value_id: {'deleted': True}
                    for value_id in MarketAttributeValue.objects.filter(
                        dictionary_id=cpu_attribute.dictionary_id
                    ).values_list('id', flat=True)
                },
            }
        )

        self.assertEqual(self.get_base_differences(), expected_diffs)

    @patch('apps.mapper.scripts.garbage_collector.prepare_objects_for_delete.mongo_garbage_insert')
    def test_prepare_mapper_objects_for_deletion(self, mock_mongo_garbage_insert):
        prepare_mapper_objects_for_deletion()

        mock_mongo_garbage_insert.asser_not_called()

        # no changes
        self.assertEqual(self.get_base_differences(), self.get_diff_template())
