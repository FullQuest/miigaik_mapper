"""Ozon related utility functions."""

import logging
from typing import Dict, Union

from apps.ozon.library.ozon_manage_offers.fetch_ozon_offers_data import (
    FetchOzonOfferData,
)


UPDATE_ERRORS: Dict[str, str] = {
    'OVER_MAX_OVH_KGT': (
        'Вес или габариты товара больше максимальных. '
        'Обновить количество не получится.'
    ),
    'OVER_MAX_OVH_NON_KGT': (
        'Вы не можете продавать крупногабаритные товары с этого склада.'
    ),
    'NON_KGT_ON_KGT_WAREHOUSE': (
        'Все ваши товары продаются как крупногабаритные, а вы хотите'
        ' продавать обычный. Он попадет под условия продажи крупногабаритных.'
    ),
    'STOCK_TOO_BIG': (
        'Указано слишком большое количество,  попробуйте уменьшить его'
    ),
    'INVALID_STATE': (
        'Товар не прошел все этапы создания, проверьте его статус'
    ),
    'CANNOT_CREATE_FBS_SKU': (
        'Произошла внутренняя ошибка при обновлении наличия,'
        ' попробуйте еще раз.'
    ),
    'NOT_FOUND_ERROR': 'Не удалось найти указанный товар',
    'SKU_STOCK_NOT_CHANGE': 'Сток товара уже соответствует указанному',
    'discount_too_big': (
        'Разница между старой и новой ценой слишком большая. '
        'Попробуйте ее уменьшить'
    ),
}


log = logging.getLogger('ozon_offer_utils')
clickhouse_logger = logging.getLogger('clickhouse_logger')


def convert_dimension_unit(unit: str):
    """Convert given dimension unit into ozon compatible."""
    dim_lookup = {'мм': 'mm', 'см': 'cm'}

    return dim_lookup.get(unit.lower(), unit)


def convert_weight_unit(unit: str):
    """Convert given weight unit into ozon compatible."""
    weight_lookup = {'г': 'g', 'кг': 'kg'}

    return weight_lookup.get(unit.lower(), unit)


def handle_offer_update_status(
        update_result: dict,
        fetcher: FetchOzonOfferData,
        update_target: str = '',
):
    """Handle offer update status.

    Write error or sucessful status to mysql, creare error description.

    :param update_result dict: dict with offer id and its update result
    :param FetchOzonOfferData fetcher: offer fetcher to update data
    """
    # WARN! Not working as intended!
    log = logging.getLogger('ozon_update_offers')
    if update_result['updated']:
        fetcher.set_ozon_update_date(
            feed_offer_id=update_result['offer_id'],
            update_target=update_target,
            errors=None,
        )

    elif not update_result['updated']:
        try:
            error = ' | '.join([
                UPDATE_ERRORS[err['code']]
                for err in update_result['errors']
            ])
        except KeyError as e:
            error = ' | '.join([
                f'{err["code"]}: {err["message"]}'
                for err in update_result['errors']
            ])

            log.warning(
                'We encountered unexpected error'
                f' while updating stocks: {e}.'
                'Please consider adding it to error descriptions.',
            )

        fetcher.set_ozon_update_date(
            feed_offer_id=update_result['offer_id'],
            errors=error,
            update_target=update_target,
        )

