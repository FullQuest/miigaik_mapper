"""OZON offers errors report processor for domain."""

import os
import csv
import json

from codecs import BOM_UTF8
from apps.utils.retry_utils import retry_on
from datetime import datetime
from typing import List, Dict, Any, Optional
from apps.utils.iterable_utils import split_to_chunks
from apps.ozon.exceptions import OzonProcessingException
from b2basket import settings
from apps.ozon.utils.api_connector.seller.api_wrapper import (
    chain_all_products,
    get_product_info_list,
)

OUTPUT_REPORT_PATH = 'ozon/reports'
FULL_OUTPUT_REPORT_PATH = os.path.join(settings.MEDIA_ROOT, OUTPUT_REPORT_PATH)
EDIT_CARD_URL_PARTS = [
    'https://seller.ozon.ru/app/products/',
    '/edit/common-attrs',
]


class OzonOffersErrorsReport:
    """Ozon offer errors report class."""

    def __init__(
        self,
        domain: str,
        offers_list: Optional[List[Dict[str, str]]] = None,
    ):
        """Set initial preparation.

        Initial values:

        :param str domain: Domain name in system
        :param offers_list: Offers

        Example of offers:

        [
            {"offer_id": "13513"},
            {"offer_id": "j133f1"},
        ]
        """
        self.domain = domain
        self.offers_list = offers_list

    def fetch_ozon_offers_list(self) -> List[Dict[str, str]]:
        """Fetch all offers list from OZON.

        If offers list not provided, processor will fetch all offers from OZON

        :return all_products_list: products list fetched from OZON
        :rtype: List[Dict[str, str]
        """
        all_products_list = list(chain_all_products(self.domain))

        if not all_products_list:
            raise OzonProcessingException('No offers fetched')

        return all_products_list

    @retry_on(OzonProcessingException, max_retry_count=3)
    def fetch_ozon_offer_info(self) -> List[Dict[str, Any]]:
        """Fetch product information from ozon including all errors.

        :return products_list: Offers info from OZON
        :rtype: List[Dict[str, Any]]
        """
        if not self.offers_list:
            self.offers_list = self.fetch_ozon_offers_list()
        ozon_products_list = self.offers_list

        offer_ids_list = [offer['offer_id'] for offer in ozon_products_list]
        offer_ids_chunks = split_to_chunks(offer_ids_list, 50)

        products_list = []

        for offer_ids_chunk in offer_ids_chunks:

            chunk_response = []

            try:
                chunk_response = get_product_info_list(
                    domain=self.domain,
                    offer_id=offer_ids_chunk,
                )
            except OzonProcessingException as err:
                if json.loads(str(err))['code'] == 8:
                    sub_chunks = split_to_chunks(offer_ids_chunk, 10)

                    for sub_chunk in sub_chunks:
                        chunk_response.extend(get_product_info_list(
                            domain=self.domain,
                            offer_id=sub_chunk,
                        ))
                else:
                    raise OzonProcessingException(str(err))

            products_list.extend(chunk_response)

        return products_list

    @staticmethod
    def remove_duplicates_from_list(input_list: list) -> list:
        """Remove duplicates from a list."""
        output_list = []
        for el in input_list:
            if el not in output_list:
                output_list.append(el)

        return output_list

    def build_report(self):
        """Build CSV formatted report for all provided offers."""
        timestamp = datetime.now()
        filename = (
            f'Ошибки_модерации_{self.domain}'
            f'_{timestamp.strftime("%d-%m-%Y")}.csv'
        )
        report_path = (
            os.path.join(
                FULL_OUTPUT_REPORT_PATH,
                filename,
            )
        )

        header = [
            'Ozon Product ID',
            'Редактировать карточку',
            'Offer ID',
            'Название карточки',
            'Статус модерации',
            'Описание ошибки',
            'Поле',
            'Кол-во ошибок в карточке',
            'Атрибут',
            'ID атрибута',
            'Код ошибки',
            'level',
        ]

        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, 'w', encoding='utf-8') as csv_file:
            csv_file.write(BOM_UTF8.decode('utf-8'))
            writer = csv.writer(csv_file, delimiter=";", lineterminator='\n')
            writer.writerow(header)

            offers_statuses_list = self.fetch_ozon_offer_info()

            for offer in offers_statuses_list:
                offer_errors = offer['errors']

                if offer_errors:

                    offer_rows = []

                    for error in offer_errors:
                        row: list = [
                            offer['id'],

                            f'{EDIT_CARD_URL_PARTS[0]}'
                            f'{offer["id"]}{EDIT_CARD_URL_PARTS[1]}',

                            offer['offer_id'],
                            offer['name'],
                            offer['statuses']['status_name'],

                            error.get('texts', {}).get('description', ''),
                            error['field'],
                            error.get('texts', {}).get('attribute_name', ''),
                            error['attribute_id'],
                            error['code'],
                            error['level'],
                        ]

                        offer_rows.append(row)

                    offer_rows_no_duplicates = \
                        self.remove_duplicates_from_list(offer_rows)

                    for row in offer_rows_no_duplicates:
                        row.insert(7, len(offer_rows_no_duplicates))

                    writer.writerows(offer_rows_no_duplicates)

        return f'/media/ozon/reports/{filename}'
