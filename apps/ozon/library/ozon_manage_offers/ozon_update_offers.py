"""Ozon offer updater."""

import logging

from typing import Any, Dict, List

import json

from apps.utils.hash_utils import hash_text
from apps.utils.iterable_utils import split_to_chunks
from apps.utils.futures_utils import concurrent_io_start

from apps.ozon.utils.api_connector.offers_ir import (
    Offer,
    convert_to_ozon_offer,
)

from .ozon_import_offers import (
    OzonOfferImporter,
    record_request_offers_data,
)

log = logging.getLogger('ozon_update_offers')

DIMENSIONS = [
    'height',
    'высота',
    'depth',
    'глубина',
    'width',
    'ширина',
]


class OzonOffersUpdater(OzonOfferImporter):
    """Class for updating offers on Ozon."""

    def update_offers(
        self,
        offers: List[Dict[str, Any]],
        feed_category_id: int,
        force: bool = False,
    ) -> Dict[str, Any]:
        """Update offers on Ozon.

        :param offers: List of Ozon offers
        :type: List[str, Any]
        :param feed_category_id: Category id in Feed
        :type: int
        :param force: Ignore last import hash
        :type: bool

        :return info: Information about update
        :rtype: Dict[str, Any]
        """
        posted_offers = [
            offer
            for offer in offers
            if offer['@id'] in self.fetcher.fetched_ozon_products_ids
        ]

        offer_import_params = self.params_manager.collect_offer_import_params(
            posted_offers,
            feed_category_id,
            initial=False,
        )

        ozon_offer_import_params: List[Offer] = []
        import_params_hashes: Dict[str, str] = {}

        for offer_import_param in offer_import_params:
            if not offer_import_param['ready_for_import']:
                continue

            self.add_new_desc_category_if_updated(
                offer_import_param=offer_import_param,
            )

            ozon_offer = convert_to_ozon_offer(offer_import_param)
            offer_hash = hash_text(
                json.dumps(ozon_offer.__dict__, sort_keys=True),
            )
            last_import_hash = self.fetcher.get_last_import_hash(
                ozon_offer.offer_id,
            )
            if not force and offer_hash == last_import_hash:
                continue
            ozon_offer_import_params.append(ozon_offer)
            import_params_hashes[ozon_offer.offer_id] = offer_hash

        product_import_infos = concurrent_io_start(
            self.batch_import_offers,
            split_to_chunks(ozon_offer_import_params, chunk_size=100),
            max_workers=5,
        )

        updated_info = []
        error_infos = []

        for product_import_info in product_import_infos:
            item = {
                'domain': self.fetcher.domain,
                'offer_ids': product_import_info['offer_ids'],
            }

            if 'error' in product_import_info:
                item['error'] = str(product_import_info['error'])
                error_infos.append(item)
            else:
                item['task_id'] = product_import_info['task_id']
                updated_info.append(item)

        for item in updated_info:
            for offer_id in item['offer_ids']:
                self.fetcher.set_ozon_offer_start_update(
                    offer_id,
                    task_id=item['task_id'],
                    import_hash=import_params_hashes[offer_id],
                )

        for item in error_infos:
            for offer_id in item['offer_ids']:
                self.fetcher.set_ozon_error_description(
                    offer_id,
                    error_description={'import-update': item['error']},
                )

        record_request_offers_data(
            domain=self.fetcher.domain,
            offers=ozon_offer_import_params,
        )

        log.info(updated_info)
        log.info(error_infos)

        return {
            'domain': self.fetcher.domain,
            'updated': updated_info,
            'error': error_infos,
        }
