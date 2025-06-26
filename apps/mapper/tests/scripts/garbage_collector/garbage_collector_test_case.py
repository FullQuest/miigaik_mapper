"""Class for testing garbage collector."""
from typing import Dict, Union
from datetime import datetime

from django.db import transaction
from django.db.models import QuerySet
from django.test import TestCase

from apps.mapper.models import (
    MarketAttributeValueDictionary,
    MarketAttributeValue,
    MarketCategoryAttribute,
    MarketAttribute,
    MarketCategory,
    Marketplace,
    ValueUnit,
)
from apps.mapper.scripts.garbage_collector.prepare_objects_for_delete import (
    MP_VALUES_COLLECTION,
    MP_DICTIONARIES_COLLECTION,
    MP_ATTRIBUTES_COLLECTION,
    MP_CATEGORY_ATTRIBUTE_COLLECTION,
    MP_CATEGORY_COLLECTION,
)
from apps.utils.mongo_utils import MongoConnMixin


ObjectModelName = str
ObjectDict = Dict[str, Union[int, str, float]]
ObjectId = int
State = Dict[ObjectModelName, Dict[ObjectId, ObjectDict]]

MONGO_MAPPER_TEST_DB = 'mapper_test'


class GarbageCollectorTestCase(TestCase):

    state_first: State = None
    deletion_threshold: datetime = None

    @staticmethod
    def _print_mongo_state():
        """Used for debug purposes."""
        conn = MongoConnMixin(MONGO_MAPPER_TEST_DB, 'test')

        values_col = getattr(conn.db, MP_VALUES_COLLECTION)
        dicts_col = getattr(conn.db, MP_DICTIONARIES_COLLECTION)
        attrs_col = getattr(conn.db, MP_ATTRIBUTES_COLLECTION)
        cat_attrs_col = getattr(conn.db, MP_CATEGORY_ATTRIBUTE_COLLECTION)
        category_col = getattr(conn.db, MP_CATEGORY_COLLECTION)

        from pprint import pprint

        pprint({
            'values': list(values_col.find({}, {'_id': 0})),
            'dicts': list(dicts_col.find({}, {'_id': 0})),
            'attrs': list(attrs_col.find({}, {'_id': 0})),
            'cat_attrs': list(cat_attrs_col.find({}, {'_id': 0})),
            'category': list(category_col.find({}, {'_id': 0})),
        })

    @staticmethod
    def drop_test_db():
        conn = MongoConnMixin(MONGO_MAPPER_TEST_DB, 'test')
        conn.client.drop_database(MONGO_MAPPER_TEST_DB)

    @classmethod
    def setUpClass(cls):
        super(GarbageCollectorTestCase, cls).setUpClass()

    def setUp(self):
        self.load_mappings()
        self.state_first = self.get_current_state()
        self.deletion_threshold = datetime.now()

    def get_base_differences(self) -> dict:
        return self.get_state_differences(
            self.state_first,
            self.get_current_state(),
        )

    @staticmethod
    def _set_deleted(obj, deleted=True):
        obj.deleted = deleted
        obj.save()

    @staticmethod
    def get_diff_template(added=None, removed=None, changed=None):
        template = {
            "added": {},
            "removed": {},
            "changed": {}
        }

        if added is not None:
            template["added"] = added

        if removed is not None:
            template["removed"] = removed

        if changed is not None:
            template["changed"] = changed

        return template

    def get_state_differences(self, state_first: State, state_second: State):
        diff = self.get_diff_template()

        for model_name, objects in state_second.items():
            diff["added"].setdefault(model_name, {})
            diff["changed"].setdefault(model_name, {})

            for obj_id, obj_data in objects.items():
                if model_name in state_first and obj_id in state_first[model_name]:
                    original_data = state_first[model_name][obj_id]
                    changes = {k: v for k, v in obj_data.items() if original_data.get(k) != v}

                    if changes:
                        diff["changed"][model_name][obj_id] = changes
                else:
                    diff["added"][model_name][obj_id] = obj_data

        for model_name, objects in state_first.items():
            diff["removed"].setdefault(model_name, set())

            for obj_id, obj_data in objects.items():
                if model_name not in state_second or obj_id not in state_second[model_name]:
                    diff["removed"][model_name].add(obj_id)

        for key in ["added", "removed", "changed"]:
            diff[key] = {
                model: objs
                for model, objs in diff[key].items()
                if objs
            }

        return diff

    @staticmethod
    def get_current_state() -> State:
        marketplaces = Marketplace.objects.all().values()
        mp_categories = MarketCategory.objects.all().values()
        mp_attributes = MarketAttribute.objects.all().values()
        mp_category_attributes = MarketCategoryAttribute.objects.all().values()
        mp_dictionaries = MarketAttributeValueDictionary.objects.all().values()
        mp_values = MarketAttributeValue.objects.all().values()

        def _id_dicts(objects: QuerySet) -> Dict[ObjectId, ObjectDict]:
            return {obj['id']: obj for obj in objects}

        return {
            'marketplaces': _id_dicts(marketplaces),
            'mp_categories': _id_dicts(mp_categories),
            'mp_attributes': _id_dicts(mp_attributes),
            'mp_category_attributes': _id_dicts(mp_category_attributes),
            'mp_dictionaries': _id_dicts(mp_dictionaries),
            'mp_values': _id_dicts(mp_values),
        }

    def load_mappings_atomic(self):
        with transaction.atomic():
            self.load_mappings()

    @staticmethod
    def load_mappings():
        """Make mapping data to cover all testing situations."""

        undelete_defaults = {"defaults": {"deleted": False}}

        # Marketplace objects creation
        mp, _ = Marketplace.objects.update_or_create(
            id=1,
            marketplace=Marketplace.OZON,
            client_id=1,
            defaults={'api_key': 'test'}
        )

        mp_parent_category, _ = MarketCategory.objects.update_or_create(
            id=1,
            marketplace=mp,
            name='pc components',
            source_id='1',
            **undelete_defaults,
        )

        mp_gpu, _ = MarketCategory.objects.update_or_create(
            id=2,
            marketplace=mp,
            parent=mp_parent_category,
            name='gpu',
            source_id='2',
            **undelete_defaults,
        )

        mp_cpu, _ = MarketCategory.objects.update_or_create(
            id=3,
            marketplace=mp,
            parent=mp_parent_category,
            name='cpu',
            source_id='3',
            **undelete_defaults,
        )

        # COMMON POWER CONSUMPTION
        # yeah, must be not dict choices attribute but numeric value, blah blah
        watts_unit, _ = ValueUnit.objects.get_or_create(id=1, name='watts')
        mp_power_con, _ = MarketAttribute.objects.update_or_create(
            id=1,
            name='power consumption',
            source_id='1',
            unit=watts_unit,
            **undelete_defaults,
        )
        MarketCategoryAttribute.objects.update_or_create(
            id=1,
            category=mp_gpu,
            attribute=mp_power_con,
            **undelete_defaults,
        )
        MarketCategoryAttribute.objects.update_or_create(
            id=2,
            category=mp_cpu,
            attribute=mp_power_con,
            **undelete_defaults,
        )

        # CPU MANUFACTURER
        cpu_manuf_dict, _ = MarketAttributeValueDictionary.objects.update_or_create(
            id=1,
            source_id='1',
            **undelete_defaults,
        )
        MarketAttributeValue.objects.update_or_create(
            id=1, dictionary=cpu_manuf_dict, value='intel', **undelete_defaults,
        )
        MarketAttributeValue.objects.update_or_create(
            id=2, dictionary=cpu_manuf_dict, value='amd', **undelete_defaults,
        )
        cpu_manuf, _ = MarketAttribute.objects.update_or_create(
            id=2,
            name='cpu_manufacturer',
            source_id='2',
            dictionary=cpu_manuf_dict,
            **undelete_defaults,
        )
        MarketCategoryAttribute.objects.update_or_create(
            id=3,
            category=mp_cpu,
            attribute=cpu_manuf,
            **undelete_defaults,
        )

        # GPU MANUFACTURER
        gpu_manuf_dict, _ = MarketAttributeValueDictionary.objects.update_or_create(
            id=2,
            source_id='2',
            **undelete_defaults,
        )
        MarketAttributeValue.objects.update_or_create(
            id=3, dictionary=gpu_manuf_dict, value='intel', **undelete_defaults,
        )
        MarketAttributeValue.objects.update_or_create(
            id=4, dictionary=gpu_manuf_dict, value='amd', **undelete_defaults,
        )
        MarketAttributeValue.objects.update_or_create(
            id=5, dictionary=gpu_manuf_dict, value='nvidia', **undelete_defaults,
        )
        gpu_manuf, _ = MarketAttribute.objects.update_or_create(
            id=3,
            name='gpu_manufacturer',
            source_id='3',
            dictionary=gpu_manuf_dict,
            **undelete_defaults,
        )
        MarketCategoryAttribute.objects.update_or_create(
            id=4,
            category=mp_gpu,
            attribute=gpu_manuf,
            **undelete_defaults,
        )
