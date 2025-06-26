"""Params manager for Ozon."""
import re
import time
import logging

from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional

from apps.mapper.utils.utils import get_mapped_values, normalize_string
from apps.ozon.library.ozon_manage_offers.fetch_ozon_offers_data import (
    FetchOzonOfferData,
)
from apps.utils.string_utils import (
    remove_prohibited_characters,
)
from apps.mapper.models import (
    MarketAttributeValue,
    CategoryMap,
    FeedMeta,
)

log = logging.getLogger('ozon_params_manager_time')

DIMENSIONS = [
    'height',
    'высота',
    'depth',
    'глубина',
    'width',
    'ширина',
]

OZON_ATTRIBUTES_IDS = {
    'group': ['8292', '10289'],
    'weight': '4497',
    'length': '9454',
    'width': '9455',
    'height': '9456',
    'model': '9048',
    'annotation': '4191',
    'name': '4180',
    'vendorCode': '9024',
}

VIDEO_URL_ID = '21841'
VIDEO_NAME_ID = '21837'
VIDEO_COMPLEX_ID = 100001

TYPE_SOURCE_ID = '8229'

COMPLEX_ATTRIBUTE_IDS = [
    VIDEO_URL_ID,
    VIDEO_NAME_ID,
]

TN_VED_CODE_SUBSTRING = 'ТН ВЭД'
MODEL_YEAR_PARAM = 'Модельный год'.upper()
MODEL_YEAR_PARAM_INT = f'{MODEL_YEAR_PARAM} INT'

VAT_BY_CODE = {
    '5': '0.0',
    '2': '0.1',
    '7': '0.2',
}


def _get(offer, key, default=None):
    value = offer.get(key)
    return default if value is None else value


def parse_dimensions(dimensions):
    """
    Parse dimensions from string.

    :param dimensions: dimensions string with '/' separator
    :type: str

    :return length, width, height: 3 separate values for length, width, height
    :rtype: tuple(str, str, str)
    """
    try:
        length, width, height = [
            str(int(float(v or '0') * 10))
            for v in dimensions.replace(',', '.').split('/')
        ]
    except Exception as e:  # noqa F841
        length, width, height = '0', '0', '0'
    return length, width, height


class OzonOfferParamsManager:
    """Class for offer params management."""

    def __init__(self, fetcher: FetchOzonOfferData):
        """Initialize attributes manager."""
        self.fetcher = fetcher
        self.ozon_category_id = None
        self.ozon_category_name = None
        self._category_mapping_data = None
        self._attribute_map = None
        self._ozon_attributes = None
        self._required_ozon_attributes: List[str] = []
        self._attributes_values = Dict[int, List[Any]]
        self._complex_attributes_values = Dict[int, List[Any]]
        self._attributes_errors = None
        self._tags_errors = Dict[str, str]
        self.initial = None

    def _get_mapping_feed_id(
        self
    ) -> Optional[int]:
        return FeedMeta.objects.get(
            domain=self.fetcher.custom_mapping_domain,
        ).id

    def _collect_offer_parameters(
        self,
        offer: dict,
    ) -> dict:
        """Collect import parameters from feed offer."""
        self._attributes_values = {}
        self._complex_attributes_values = {}
        self._required_ozon_attributes = [
            attr_source_id for attr_source_id, attr_data
            in self._ozon_attributes.items() if attr_data['required']
        ]
        self._tags_errors = {}

        name = ''

        if '@name' in offer:
            name = offer['@name']

        elif 'name' in offer and offer['name']:
            name = offer['name']

        elif 'model' in offer and offer['model']:
            name = offer['model']

        if not name:
            self._tags_errors['@name/name/model'] = 'missing'

        valid_name = remove_prohibited_characters(name)

        depth, width, height = None, None, None
        if offer.get('dimensions'):
            try:
                depth, width, height = \
                    parse_dimensions(offer.get('dimensions'))
            except Exception:
                self._tags_errors['dimensions'] = 'bad_value'
        else:
            self._tags_errors['dimensions'] = 'missing'

        weight = _get(offer, 'weight', 0)
        try:
            weight = float(str(weight).replace(',', '.')) * 1000
        except ValueError as e:  # noqa F841
            self._tags_errors['weight'] = 'bad_value'

        images = _get(offer, 'picture', [])
        if not isinstance(images, list):
            images = [images]
        if not images:
            self._tags_errors['picture'] = 'missing'
        elif not images[0]:
            self._tags_errors['picture'] = 'empty'

        images360 = offer.get('images360')
        if not images360 or not isinstance(images360, str):
            images360 = []
        else:
            images360 = images360.split(',')

        self._collect_attributes(offer)

        vat = _get(offer, 'vat', 'VAT_20')

        if vat in (None, ''):
            self._tags_errors['vat'] = 'missing'
        elif vat == 'NO_VAT':
            vat = '0'
        elif vat in VAT_BY_CODE:
            vat = VAT_BY_CODE[vat]
        else:
            try:
                vat = str(int(vat.split('_')[-1]) / 100)
            except ValueError:
                self._tags_errors['vat'] = 'bad_value'

        price = offer.get('price')
        if price is None:
            self._tags_errors['price'] = 'missed'

        if self._category_mapping_data['market_category_deleted']:
            self._tags_errors['Категория'] = 'mapped_with_deleted'

        video_attr = self._complex_attributes_values.get(VIDEO_COMPLEX_ID, {})

        if (
            int(VIDEO_URL_ID) in video_attr
            and int(VIDEO_NAME_ID) not in video_attr
        ):
            self.add_name_to_complex_video(valid_name)

        info = {
            'barcode': _get(offer, 'barcode', ''),
            'category_id': self.ozon_category_id,
            'name': valid_name,
            'offer_id': offer['@id'],
            'price': price,
            'old_price': _get(offer, 'oldprice', ''),
            'vat': vat,
            'height': height,
            'depth': depth,
            'width': width,
            'dimension_unit': 'mm',
            'weight': weight,
            'weight_unit': 'g',
            'images': images[1:15],
            'primary_image': images[0] if images else '',
            'images360': images360,
            'attributes': list(self._attributes_values.values()),
            'complex_attributes': self.convert_complex_attributes(),
            'attributes_errors': self._attributes_errors,
            'tags_errors': self._tags_errors,
            'ozon_attributes_data': self._ozon_attributes,
            'ready_for_import': (
                not self._required_ozon_attributes and  # noqa W504
                not self._tags_errors
            ),
        }

        return info

    def add_name_to_complex_video(self, name):
        """Add name to _complex_attributes_values if URL provided without it

        Currently, there are no way to upload video without name provided.
        """
        self._complex_attributes_values[VIDEO_COMPLEX_ID][VIDEO_NAME_ID] = {
            'complex_id': VIDEO_COMPLEX_ID,
            'id': int(VIDEO_NAME_ID),
            'values': [{'value': name}],
        }

    def convert_complex_attributes(self):
        """Undictify provided complex attributes."""
        complex_attributes = [
            {"attributes": list(complex_values.values())}
            for complex_id, complex_values
            in self._complex_attributes_values.items()
        ]
        return complex_attributes

    def collect_offer_import_params(
        self,
        offers: List[Dict[str, Any]],
        feed_category_id: int,
        initial: bool = True,
    ) -> List[Dict[str, Any]]:
        start_time = time.time()
        """Collect params required for offer import.

        :param offers: List of not yet posted Ozon offers
        :type: List[Dict[str, Any]]

        :param str feed_category_id: Feed category ID

        :param bool initial: True if initial offer import

        :return offer_import_params: Params for offer import
        :rtype: List[Dict[str, Any]]
        """
        self.initial = initial
        offer_import_params: List[Dict[str, Any]] = []
        self._category_mapping_data = self.fetcher.category_map[
            feed_category_id
        ]
        self.ozon_category_id = self._category_mapping_data[
            'market_category_id'
        ]
        self.ozon_category_name = CategoryMap.objects.get(
            feed_category__source_id=feed_category_id,
            feed_category__feed_id=self._get_mapping_feed_id(),
            marketplace_category__source_id=self.ozon_category_id,
        ).marketplace_category.name
        self._attribute_map = self.fetcher.get_category_attribute_map(
            category_mapping_id=self._category_mapping_data['mapping_id'],
        )
        self._ozon_attributes = self.fetcher.get_ozon_category_attributes(
            self.ozon_category_id,
        )
        cycle_start = time.time()
        for offer in offers:
            offer_import_params.append(
                self._collect_offer_parameters(offer),
            )
        end_time = time.time()
        result_time = end_time - start_time
        cycle_time = end_time - cycle_start
        log.error(
            f'{self.fetcher.domain}: COUNT OFFERS - {len(offers)} CYCLE TIME - {cycle_time}'
            f'  COLLECT TIME - {result_time}'
        )
        return offer_import_params

    def _add_attribute_value(
        self,
        attribute_source_id: str,
        value: str,
        dictionary_value_id: str = '0',
    ):
        self._attributes_values.setdefault(
            int(attribute_source_id),
            {
                "complex_id": 0,
                "id": int(attribute_source_id),
                "values": [],
            },
        )['values'].append({
            'value': value,
            'dictionary_value_id': int(dictionary_value_id),
        })
        self._remove_ozon_attribute_from_required(attribute_source_id)

    def _add_complex_attribute_value(
        self,
        attribute_source_id: str,
        value: str,
        complex_id: int,
    ):
        self._complex_attributes_values.setdefault(
            complex_id, {}
        ).setdefault(
            int(attribute_source_id),
            {
                "complex_id": complex_id,
                "id": int(attribute_source_id),
                "values": [],
            },
        )['values'].append({
            'value': value,
        })
        self._remove_ozon_attribute_from_required(attribute_source_id)

    def _convert_attribute_value(self, value, _type):
        if _type == 'Decimal':
            return str(float(value.replace(',', '.')))
        if _type == 'Integer':
            return str(int(value))
        # if _type == 'Boolean':
        #     return value == 'true'
        return remove_prohibited_characters(value)

    def _clear_annotation_tags(self, text):
        """Remove Ozon annotation unsupported tags."""
        soup = BeautifulSoup(text, 'lxml')

        for el in soup.findAll(text=False):
            if el.name == 'ol':
                el.name = 'ul'
                continue
            if el.name in ['body', 'br', 'ul', 'li']:
                continue
            if el.name in ['h1', 'h2', 'h3', 'p']:
                el.insert_before(soup.new_tag('br'))
                el.insert_after(soup.new_tag('br'))
            el.unwrap()

        result_text = ''.join(map(str, soup.body.contents)).strip()

        break_tag = '<br/>'
        if result_text.startswith(break_tag):
            result_text = result_text[len(break_tag):]

        return result_text

    def _extract_tn_ved_code(self, code: str):
        result = re.search(r'(?P<code>\d[\d .]{2,}\d)', code)
        if not result:
            return ''
        return result.group('code').replace(' ', '')

    def _remove_ozon_attribute_from_required(self, source_id: str):
        while source_id in self._required_ozon_attributes:
            self._required_ozon_attributes.remove(source_id)

    def _add_tn_ved_codes(self, offer):
        tn_ved_codes = _get(offer, 'tn-ved-codes', {})
        if isinstance(tn_ved_codes, dict):
            tn_ved_codes = tn_ved_codes.get('tn-ved-code', [])
        if not isinstance(tn_ved_codes, list):
            tn_ved_codes = [tn_ved_codes]

        tn_ved_attributes = {
            source_id: data
            for source_id, data
            in self._ozon_attributes.items()
            if TN_VED_CODE_SUBSTRING in data['name']
        }

        for ozon_attr_id, data in tn_ved_attributes.items():
            values_by_code = {}
            for attr_value in MarketAttributeValue.objects.filter(
                dictionary_id=data['dictionary_id'],
                deleted=False,
            ):
                values_by_code[self._extract_tn_ved_code(attr_value.value)] = {
                    'value': attr_value.value,
                    'dictionary_value_id': attr_value.source_id,
                }

            for tn_ved_code in tn_ved_codes:
                tn_ved_code_part = str(tn_ved_code)
                while len(tn_ved_code_part) >= 4:
                    if tn_ved_code_part in values_by_code:
                        self._add_attribute_value(
                            ozon_attr_id,
                            **values_by_code[tn_ved_code_part],
                        )
                        break
                    tn_ved_code_part = tn_ved_code_part[:-1]
                else:
                    if len(tn_ved_code_part) == 3:
                        tn_ved_code_part = f'{tn_ved_code[:4]}00'
                        if tn_ved_code_part in values_by_code:
                            self._add_attribute_value(
                                ozon_attr_id,
                                **values_by_code[tn_ved_code_part],
                            )

        errors = {}
        for source_id in tn_ved_attributes:
            if source_id not in self._required_ozon_attributes:
                continue
            if not tn_ved_codes:
                errors[source_id] = 'missing'
            else:
                errors[source_id] = 'not_found'
        return {'errors': errors}

    def _collect_attributes(self, offer):
        """Get attributes values."""
        errors = {}

        def add_error(source_id: int, error: str):
            if source_id not in self._ozon_attributes:
                return
            if not self._ozon_attributes[source_id]['required']:
                return
            errors[source_id] = error

        offer_params = _get(offer, 'param', [])
        if not isinstance(offer_params, list):
            offer_params = [offer_params]

        feed_params_values = {}

        for param in offer_params:
            if not param:
                continue
            feed_params_values.setdefault(
                normalize_string(param['@name']),
                [],
            ).append(param.get('#text', '').strip())
        for tag, value in offer.items():
            if isinstance(value, (str, bool, int, float)):
                feed_params_values.setdefault(
                    tag.strip().upper(),
                    [],
                ).append(
                    str(value.strip() if isinstance(value, str) else value),
                )

        if MODEL_YEAR_PARAM in feed_params_values:
            model_years = []
            for model_year in feed_params_values[MODEL_YEAR_PARAM]:
                ozon_model_year = ('20' + model_year.split('-')[-1])[-4:]
                model_years.append(ozon_model_year)
            feed_params_values[MODEL_YEAR_PARAM_INT] = model_years

        if MODEL_YEAR_PARAM in self._attribute_map:
            for mapping_id in list(
                self._attribute_map[MODEL_YEAR_PARAM].keys(),
            ):
                mapping_data = self._attribute_map[MODEL_YEAR_PARAM][
                    mapping_id
                ]
                if 'values_map' not in mapping_data:
                    self._attribute_map.setdefault(
                        MODEL_YEAR_PARAM_INT,
                        {},
                    )[mapping_id] = mapping_data
                    self._attribute_map[MODEL_YEAR_PARAM].pop(mapping_id)

        marketplace_attributes_values = get_mapped_values(
            self._attribute_map,
            feed_params_values,
        )

        for values_data in marketplace_attributes_values['values'].values():
            for value_data in values_data:

                val_source_id = f'{value_data["attribute_source_id"]}'

                if val_source_id in COMPLEX_ATTRIBUTE_IDS:

                    if val_source_id == VIDEO_URL_ID:
                        value_data["value"] = process_video_url(
                            value_data["value"],
                        )

                    self._add_complex_attribute_value(
                        value_data["attribute_source_id"],
                        value_data["value"],
                        VIDEO_COMPLEX_ID,
                    )
                    continue

                if (
                    value_data['dictionary_value_id'] == 0 and not
                    value_data['is_rich_content'] and not
                    value_data['ignore_data_type']
                ):
                    try:
                        value_data['value'] = self._convert_attribute_value(
                            value_data['value'],
                            value_data['data_type'],
                        )
                    except ValueError:
                        add_error(
                            value_data['attribute_source_id'],
                            'bad_value',
                        )
                        break
                value_data.pop('data_type')
                value_data.pop('is_rich_content')
                value_data.pop('deleted', None)
                value_data.pop('ignore_data_type')
                self._add_attribute_value(**value_data)

        for attribute_source_id in marketplace_attributes_values[
            'mapped_with_deleted_value'
        ]:
            add_error(attribute_source_id, 'mapped_with_deleted_value')

        for attribute_source_id in marketplace_attributes_values[
            'unmapped_value_attributes'
        ]:
            add_error(attribute_source_id, 'unmapped_val')

        for attribute_source_id in marketplace_attributes_values[
            'empty_value_attributes'
        ]:
            add_error(attribute_source_id, 'empty')

        for attribute_source_id in marketplace_attributes_values[
            'type_error_attributes'
        ]:
            add_error(attribute_source_id, 'bad_value')

        def add_if_exists(attribute_key, value):
            attribute_source_ids = OZON_ATTRIBUTES_IDS[attribute_key]
            if not isinstance(attribute_source_ids, list):
                attribute_source_ids = [attribute_source_ids]
            for attribute_source_id in attribute_source_ids:
                if attribute_source_id not in self._ozon_attributes:
                    continue
                if value is None:
                    add_error(attribute_source_id, 'missing')
                    return
                if value == '':
                    add_error(attribute_source_id, 'empty')
                self._add_attribute_value(attribute_source_id, value)

        name = offer.get('name')
        if not name:
            name_parts = map(
                lambda tn: offer.get(tn),
                ['typePrefix', 'vendor', 'model'],
            )
            if all(name_parts):
                name = ' '.join(name_parts)

        add_if_exists('name', name)

        add_if_exists('vendorCode', offer.get('vendorCode'))

        add_if_exists(
            'group',
            offer.get('@group_id', f'b2b-{offer["@id"]}'),
        )

        length, width, height = parse_dimensions(offer.get('dimensions'))

        add_if_exists('length', length)
        add_if_exists('width', width)
        add_if_exists('height', height)

        weight = _get(offer, 'weight', 0)
        try:
            weight = str(float(str(weight).replace(',', '.')) * 1000)
            add_if_exists(
                'weight',
                weight,
            )
        except ValueError:
            if OZON_ATTRIBUTES_IDS['weight'] in self._required_ozon_attributes:
                errors[OZON_ATTRIBUTES_IDS['weight']] = 'bad_value'

        add_if_exists('model', offer.get('model'))

        if offer.get('description'):
            add_if_exists(
                'annotation',
                self._clear_annotation_tags(
                    remove_prohibited_characters(offer.get('description')),
                ),
            )

        tn_ved_result = self._add_tn_ved_codes(offer)
        if tn_ved_result['errors']:
            errors.update(tn_ved_result['errors'])

        # Type id processing
        self._remove_ozon_attribute_from_required(TYPE_SOURCE_ID)

        mappings = {}
        for attr_name, attr_maps in self._attribute_map.items():
            for map_id, attr_map in attr_maps.items():
                mappings[attr_map['source_id']] = {
                    'name': attr_name,
                    'map_id': map_id,
                }
        for required_attr_source_id in self._required_ozon_attributes:
            if required_attr_source_id in errors:
                continue
            required_attribute_data = self._ozon_attributes[
                required_attr_source_id
            ]
            if required_attribute_data['disabled']:
                errors.setdefault(required_attr_source_id, 'logical')
                continue
            if required_attr_source_id not in mappings:
                errors[required_attr_source_id] = 'unmapped'
                continue
            feed_attribute_name = mappings[required_attr_source_id]['name']
            if feed_attribute_name not in feed_params_values:
                errors[required_attr_source_id] = 'missing'
                continue

            errors[required_attr_source_id] = 'unknown err'

        self._attributes_errors = errors


def process_video_url(attr_url: str) -> str:
    """Convert url if contains only YouTube code, or shorten YT url."""
    base_url = 'youtube.com/watch?v='
    base_url_full = 'https://www.youtube.com/watch?v='
    shorten_base_url = 'youtu.be/'

    if '.' not in attr_url:
        return f'{base_url_full}{attr_url}'

    if base_url in attr_url:
        return (
            f'{base_url_full}'
            f'{attr_url.split(base_url)[1]}'
        )

    if shorten_base_url in attr_url:
        return (
            f'{base_url_full}'
            f'{attr_url.split(shorten_base_url)[1]}'
        )

    return attr_url


class ParamsConstructor:
    """Class for offer manipulations."""

    def __init__(
        self,
        fetcher: FetchOzonOfferData,
        params_manager: OzonOfferParamsManager,
    ):
        """Initialize ParamsConstructor instance."""
        self.fetcher = fetcher
        self.params_manager = params_manager
