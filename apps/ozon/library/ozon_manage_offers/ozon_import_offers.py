"""Import offers to Ozon."""
import json
import logging

from typing import Any, Dict, List, Union
from django.utils import timezone

from apps.ozon.models import OzonOffer
from apps.ozon.utils.api_connector.seller.api_wrapper import product_import
from apps.ozon.utils.api_connector.offers_ir import (
    convert_to_ozon_offer,
    convert_offer_to_item,
    Offer,
)
from apps.utils.futures_utils import concurrent_io_start
from apps.utils.hash_utils import hash_text
from apps.utils.iterable_utils import split_to_chunks
from apps.ozon.library.ozon_manage_offers.params_manager import (
    ParamsConstructor,
)


log = logging.getLogger('ozon_import_offers')


class OzonOfferImporter(ParamsConstructor):
    """Class to import offers on Ozon."""

    def import_offers(
        self,
        offers: List[Dict[str, Any]],
        feed_category_id: int,
    ) -> List[Dict[str, Any]]:
        """Import offers to Ozon.

        :param offers: List of offers from feed
        :type: List[Dict[str, Any]]

        :param int feed_category_id: Feed category ID

        :return info: Information about offer import
        :rtype: Dict[str, Any]
        """
        not_posted_offers = [
            offer
            for offer in offers
            if offer['@id'] not in self.fetcher.fetched_ozon_products_ids
        ]

        offer_import_params = self.params_manager.collect_offer_import_params(
            not_posted_offers,
            feed_category_id,
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

            ozon_offer_import_params.append(ozon_offer)
            import_params_hashes[ozon_offer.offer_id] = offer_hash

        product_import_infos = concurrent_io_start(
            self.batch_import_offers,
            split_to_chunks(ozon_offer_import_params, chunk_size=100),
            max_workers=5,
        )

        info = []
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
                info.append(item)

        for item in info:
            for offer_id in item['offer_ids']:
                self.fetcher.set_ozon_offer_start_import(
                    feed_offer_id=offer_id,
                    task_id=item['task_id'],
                    import_hash=import_params_hashes[offer_id],
                )

        for item in error_infos:
            for offer_id in item['offer_ids']:
                self.fetcher.set_ozon_error_description(
                    offer_id,
                    error_description={'import': item['error']},
                )

        record_request_offers_data(
            domain=self.fetcher.domain,
            offers=ozon_offer_import_params,
        )

        log.info(info)
        log.info(error_infos)

        return info

    def batch_import_offers(
        self,
        offer_batch: List[Offer],
    ) -> Dict[str, Union[int, List[int], Exception]]:
        """Import up to 100 offers to Ozon.

        :param offer_batch: List of Offer data class objects
        :type: List[Offer]

        :return product_import_info: Code of product import task on Ozon and
                                     imported offers ID`s
        :rtype: Dict[str, Union[int, List[int]]]
        """
        product_import_info = product_import(self.fetcher.domain, offer_batch)

        return product_import_info

    def add_new_desc_category_if_updated(
        self,
        offer_import_param: Dict[str, Any],
    ):
        """
        If mapper offer category not the same as ozon category,
        we must specify new category in "new_description_category_id" tag

        :param offer_import_param: Params for offer import
        :type: Dict[str, Any]
        """
        category_and_type_on_ozon = self.fetcher.ozon_category_info.get(
            offer_import_param['offer_id'],
        )

        if not category_and_type_on_ozon:
            return

        current_ozon_category = (
            f"{category_and_type_on_ozon['description_category_id']}_"
            f"{category_and_type_on_ozon['type_id']}"
        )

        if current_ozon_category != offer_import_param['category_id']:
            offer_import_param['new_description_category_id'] = (
                offer_import_param['category_id'].split('_')[0]
            )


def record_request_offers_data(
    domain: str,
    offers: List[Offer],
):
    """Record offer import request data to MySQL OzonOffer.

    :param str domain: OzonAuthKey domain
    :param List[Offer] offers: List of Offer dataclass objects.
    """
    for offer in offers:

        ozon_offer_query = OzonOffer.objects.filter(
            domain_id=domain,
            feed_offer_id=offer.offer_id,
        )

        if not ozon_offer_query:
            continue

        if len(ozon_offer_query) > 1:
            log.warning(
                'more than one offers returned for '
                f'domain: {domain}, '
                f'offer_id: {offer.offer_id}',
            )
            continue

        ozon_offer = ozon_offer_query[0]

        offer_import_data = {
            'items': [convert_offer_to_item(offer)],
        }

        offer_import_json = json.dumps(offer_import_data, indent=4)

        ozon_offer.last_import_request_data = offer_import_json
        ozon_offer.last_offer_update = timezone.now()
        ozon_offer.save()
