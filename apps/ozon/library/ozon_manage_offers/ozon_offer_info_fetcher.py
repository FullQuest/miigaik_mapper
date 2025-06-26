"""Ozon offer state fetcher."""
import itertools
import logging
from datetime import datetime, timedelta

from typing import List

from apps.ozon.utils.api_connector.seller.api_wrapper import (
    chain_all_products,
    get_product_info_list,
)
from apps.utils.futures_utils import concurrent_io_start
from apps.utils.iterable_utils import split_to_chunks

from .params_manager import ParamsConstructor

log = logging.getLogger('ozon_offer_state_fetcher')

PROCESSED = 'approved'


class OzonOfferInfoFetcher(ParamsConstructor):
    """Class for imported offer state fetching from Ozon.

    :return ozon_imported_offers_state_data: Operation result
    :rtype: List[object]
    """

    def fetch_imported_offers_info(self) -> List[object]:
        """Fetch imported offers info from OZON."""
        imported_offers_info_storage: List[object] = []

        processed_offers = self.fetcher.processed_offers_ids
        unprocessed_offers = self.fetcher.unprocessed_offers_ids

        for offer_id in unprocessed_offers:
            imported_offers_info = (
                get_product_info_list(
                    domain=self.fetcher.domain,
                    offer_id=[offer_id],
                    product_id=None,
                    sku=None,
                    trace_requests=True,
                )[0]
            )

            if imported_offers_info['statuses']['moderate_status'] == PROCESSED:
                imported_offers_info_storage.append(
                    self.fetcher.set_ozon_offer_state(
                        feed_offer_id=offer_id,
                        state=imported_offers_info['statuses']['moderate_status'],
                        is_processed=True,
                    ),
                )
            else:
                imported_offers_info_storage.append(
                    self.fetcher.set_ozon_offer_state(
                        feed_offer_id=offer_id,
                        state=imported_offers_info['statuses']['moderate_status'],
                    ),
                )


            self.fetcher.set_ozon_product_id(
                offer_id,
                imported_offers_info['id'],
            )

            imported_offers_info_storage.append(
                self.fetcher.set_ozon_error_description(
                    feed_offer_id=offer_id,
                    error_description=imported_offers_info['errors'],
                ),
            )

        for offer_id in processed_offers:
            processed_offer_info = get_product_info_list(
                domain=self.fetcher.domain,
                offer_id=[offer_id],
                product_id=None,
                sku=None,
                trace_requests=True,
            )[0]
            self.fetcher.set_ozon_product_id(
                offer_id,
                processed_offer_info['id'],
            )

        return imported_offers_info_storage

    def batch_get_product_info_list(
        self,
        offer_ids: List[str],
    ):
        """Batch get product info list."""
        return get_product_info_list(
            self.fetcher.domain,
            offer_id=offer_ids,
        )

    def fetch_offers_fbs_sku(self, only_outdated: bool = False):
        """Fetch offers fbs_sku info from OZON."""
        offer_ids = [
            offer['offer_id']
            for offer in chain_all_products(self.fetcher.domain)
            if 'offer_id' in offer
        ]
        if only_outdated:
            fresh_offer_ids = [
                offer['@id'] for offer in self.fetcher.offers_data.find(
                    {
                        'fbs_sku_updated': {
                            '$gt': datetime.now() - timedelta(days=1),
                        },
                    },
                )
            ]

            offer_ids = list(set(offer_ids) - set(fresh_offer_ids))

        for offer in itertools.chain(*concurrent_io_start(
            self.batch_get_product_info_list,
            split_to_chunks(offer_ids, chunk_size=50),
            max_workers=10,
        )):
            self.fetcher.offers_data.update_one(
                {'@id': offer['offer_id']},
                {'$set': {
                    'fbs_sku': 0,
                    'fbs_sku_updated': datetime.now(),
                }},
                upsert=True,
            )

    def fetch_offers_fbs_sku_by_offer_ids(self, offer_ids: List[str]):
        """Fetch offers by given offer_ids list."""
        for offer in itertools.chain(*concurrent_io_start(
            self.batch_get_product_info_list,
            split_to_chunks(offer_ids, chunk_size=50),
            max_workers=10,
        )):
            self.fetcher.offers_data.update_one(
                {'@id': offer['offer_id']},
                {'$set': {
                    'fbs_sku': 0,
                    'fbs_sku_updated': datetime.now(),
                }},
                upsert=True,
            )
