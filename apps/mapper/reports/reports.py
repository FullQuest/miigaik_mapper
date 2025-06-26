"""reports for mapper."""

import os
from codecs import BOM_UTF8
from csv import DictWriter
from datetime import datetime
from collections import OrderedDict

from django.forms import model_to_dict

from apps.mapper.models import (
    FeedMeta,
    CategoryMap,
    Marketplace,
)

from apps.ozon.library import OzonManageOffers
from apps.mapper.utils.utils import get_market_category_attributes
from b2basket import settings

OUTPUT_REPORT_PATH = 'mapper/reports'
FULL_OUTPUT_REPORT_PATH = os.path.join(settings.MEDIA_ROOT, OUTPUT_REPORT_PATH)

ERRORS_DESCRIPTION = {
    'unmapped': 'Не смапплен атрибут',
    'missing': 'Не найден атрибут в фиде',
    'unmapped_val': 'Не смапплено значение атрибута',
    'empty': 'Пустое значение в фиде',
    'bad_value': 'Плохое значение. Например не преобразуется в число.',
    'not_found': (
        'Ошибка автоматического маппинга.'
        'Неизвестное значение в фиде.'
    ),
    'mapped_with_deleted': 'Смапленный объект удален',
    'mapped_with_deleted_value': 'Смапленное значение удалено',
}


class FeedMapperReport:
    """Mapper feed report class."""

    def __init__(self, marketplace_id, feed_id):
        """Set initial preparation."""
        self.marketplace = Marketplace.objects.get(id=marketplace_id)
        self.feed = FeedMeta.objects.get(id=feed_id)

    def build_report(self) -> str:
        """Build mapping report."""
        if self.marketplace.marketplace == 'ozon':
            offers_manager = OzonManageOffers(
                domain=self.feed.domain,
            )
            report_data = offers_manager.get_required_attributes_report_data()

        else:
            raise Exception('Integration is not implemented')

        market_category_data = {}
        feed_category_data = {}
        market_attributes_data = {}
        for category_map in CategoryMap.objects.filter(
            feed_category__feed=self.feed,
            marketplace_category__marketplace=self.marketplace,
        ).select_related(
            'feed_category',
            'marketplace_category',
        ):
            market_category = category_map.marketplace_category
            market_category_data[
                str(market_category.source_id)] = model_to_dict(
                market_category,
                fields=['name'],
            )

            feed_category = category_map.feed_category
            feed_category_data[feed_category.source_id] = model_to_dict(
                feed_category,
                fields=['name'],
            )

            market_attributes_data.update(
                get_market_category_attributes(
                    self.marketplace.marketplace,
                    market_category.source_id,
                ),
            )

        timestamp = datetime.now()
        filename = (
            f'{self.marketplace.marketplace}'
            f'_{self.feed.domain}_report'
            f'_{timestamp.strftime("%Y_%m_%d_%H_%M_%S")}.csv'
        )
        report_path = (
            os.path.join(
                FULL_OUTPUT_REPORT_PATH,
                filename,
            )
        )

        attribute_names = set()
        tag_names = set()

        for category_data in report_data:
            for offer_data in category_data['offers']:
                attribute_names.update(offer_data['attributes_errors'])
                tag_names.update(offer_data['tags_errors'])

        const_fieldnames = OrderedDict([
            ('feed_cat_id', 'ID категории фида'),
            ('feed_cat_name', 'Категория фида'),
            ('market_cat_id', 'ID категории МП'),
            ('market_cat_name', 'Категория МП'),
            ('offer_id', 'ID товара'),
            ('offer_name', 'Название товара'),
            ('ready', 'Готов'),
        ])

        fieldnames = list(const_fieldnames.values()) + \
            sorted(tag_names) + sorted(attribute_names)

        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w', newline='') as report_file:
            report_file.write(BOM_UTF8.decode('utf-8'))
            writer = DictWriter(
                report_file,
                fieldnames=fieldnames,
                delimiter=';',
            )
            writer.writeheader()

            for category_data in report_data:
                feed_category_id = category_data['feed_category_id']
                feed_category_name = feed_category_data[
                    feed_category_id
                ]['name']
                market_category_id = (
                    category_data['market_category_id']
                    if self.marketplace.marketplace in ['ozon']
                    else category_data['market_category_name']
                )

                market_category_name = market_category_data[
                    market_category_id
                ]['name']
                for offer_data in category_data['offers']:
                    const_field_values = [
                        feed_category_id,
                        feed_category_name,
                        market_category_id,
                        market_category_name,
                        offer_data['id'],
                        offer_data['name'],
                        'нет' if (
                            offer_data['tags_errors']
                            or offer_data['attributes_errors']
                        ) else 'да',
                    ]

                    row_values = {
                        const_fieldnames[key]: value
                        for key, value in zip(
                            const_fieldnames,
                            const_field_values,
                        )
                    }
                    for errors_key in ('tags_errors', 'attributes_errors'):
                        row_values.update({
                            key: ERRORS_DESCRIPTION.get(value, value)
                            for key, value in offer_data[errors_key].items()
                        })

                    writer.writerow(row_values)
        return f'/media/mapper/reports/{filename}'
