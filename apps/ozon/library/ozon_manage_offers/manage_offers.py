"""Add and update offers on Ozon."""
import logging

from functools import partial
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

from pprint import pprint
from typing import Any, Callable, Dict, Generator, List, Optional

from apps.ozon.exceptions import MongoReverseException, OzonProcessingException
from apps.mapper.utils.utils import update_mapping_sync_date
from apps.ozon.models import OzonOffer

from apps.ozon.library.ozon_manage_offers.fetch_ozon_offers_data import FetchOzonOfferData
from apps.ozon.library.ozon_manage_offers.ozon_generate_error_report import OzonGenerateErrorReport
from apps.ozon.library.ozon_manage_offers.ozon_import_offers import OzonOfferImporter
from apps.ozon.library.ozon_manage_offers.ozon_import_status_checker import OzonImportStatusChecker
from apps.ozon.library.ozon_manage_offers.ozon_offer_info_fetcher import OzonOfferInfoFetcher
from apps.ozon.library.ozon_manage_offers.ozon_update_offers import OzonOffersUpdater
from apps.ozon.library.ozon_manage_offers.params_manager import OzonOfferParamsManager

log = logging.getLogger('ozon_manage_offers')

OfferMeta = namedtuple(
    'OfferInfo',
    ['offers', 'feed_category_id', 'ozon_category_id'],
)


class OzonManageOffers:
    """Class for posting and updating offers on Ozon."""

    def __init__(
        self,
        domain: str,
        feed_offer_ids: Optional[List[str]] = None,
        ozon_product_ids: Optional[List[int]] = None,
        **kwargs,
    ):
        """Mutual kwargs with MongoConnMixin."""
        if feed_offer_ids and ozon_product_ids:
            raise OzonProcessingException(
                "Can't use both feed offer id's and Ozon product id's!",
            )

        database = kwargs.pop('dbname', 'ozon')

        self.fetcher: FetchOzonOfferData = FetchOzonOfferData(
            database,
            domain,
            **kwargs,
        )
        self.params_manager = OzonOfferParamsManager(self.fetcher)

        self.offer_importer = \
            OzonOfferImporter(self.fetcher, self.params_manager)
        self.offers_updater = \
            OzonOffersUpdater(self.fetcher, self.params_manager)
        self.import_status_checker = \
            OzonImportStatusChecker(self.fetcher, self.params_manager)
        self.offers_info_fetcher = \
            OzonOfferInfoFetcher(self.fetcher, self.params_manager)
        self.error_report_generator = \
            OzonGenerateErrorReport(self.fetcher, self.params_manager)

        self.disabled = self.fetcher.domain_settings.get('disabled', True)
        self.full_update_allowed = self.fetcher.domain_settings.get(
            'full_update_allowed', False,
        )
        self.enable_price_update = self.fetcher.domain_settings.get(
            'enable_price_update', False,
        )
        self.enable_stock_update = self.fetcher.domain_settings.get(
            'enable_stock_update', False,
        )
        self.enable_stock_reset = self.fetcher.domain_settings.get(
            'enable_stock_reset', False,
        )
        self.enable_posting = self.fetcher.domain_settings.get(
            'enable_posting', False,
        )
        self.enable_sync_with_feed = self.fetcher.domain_settings.get(
            'enabled_sync_with_feed', False,
        )

        if ozon_product_ids:
            feed_offer_ids = list(
                OzonOffer.objects
                .filter(product_id__in=ozon_product_ids)
                .values_list('feed_offer_id', flat=True),
            )

        self.fetched_offers: List[Dict[str, Any]] = []

        if feed_offer_ids:
            self.fetched_offers = list(
                self.fetcher.offers.find({'@id': {'$in': feed_offer_ids}}),
            )

        self.update_descriptions_on_ozon = partial(
            self.update_offers_on_ozon,
            only_description=True,
        )


    @property
    def all_feed_offers(self) -> List[Dict[str, Any]]:
        """Return all feed offers."""
        if self.fetched_offers:

            return self.fetched_offers

        return list(self.fetcher.offers.find())


    def generate_meta_offers(self) -> Generator[OfferMeta, None, None]:
        """Offer generator.

        :return: Offer generator
        :rtype: Generator[OfferMeta, None]
        """
        for feed_category_id in self.fetcher.feed_categories_ids:

            if feed_category_id not in self.fetcher.category_map:
                continue
            ozon_category_id = self.fetcher.category_map[feed_category_id][
                'market_category_id'
            ]

            try:
                offers = self.fetcher.get_feed_offers(feed_category_id)

            except MongoReverseException:
                log.warning(
                    f'No offers for category {feed_category_id} '
                    f'in {self.fetcher.domain}.\n'
                    f'traceback',
                )

                continue

            yield OfferMeta(offers, feed_category_id, ozon_category_id)

    def update_offers_on_ozon(self, force: bool = False):
        """Update all offers."""
        if not self.full_update_allowed and not self.fetched_offers:
            raise OzonProcessingException(
                f'Full update is not allowed for {self.fetcher.domain}',
            )

        for info in self.generate_meta_offers():
            self.offers_updater.update_offers(
                info.offers,
                info.feed_category_id,
                force=force,
            )
        update_mapping_sync_date(self.fetcher.domain, 'ozon')


    def import_offers_to_ozon(self):
        """Post offers."""
        for info in self.generate_meta_offers():
            self.offer_importer.import_offers(
                info.offers,
                info.feed_category_id,
            )
        update_mapping_sync_date(self.fetcher.domain, 'ozon')

    def check_import_status(self):
        """Check offer import statuses."""
        self.import_status_checker.check_import_status()

    def fetch_imported_offers_info(self):
        """Fetch imported offers info."""
        self.offers_info_fetcher.fetch_imported_offers_info()

    def process_fetched_offers(self, function: Callable[[dict], None]):
        """Process fetched offers."""
        total_offers = len(self.fetched_offers)
        processed_offers = 0

        with ThreadPoolExecutor(50) as pool:
            for ready_offer in pool.map(function, self.fetched_offers):
                if ready_offer:
                    processed_offers += 1

        if processed_offers != total_offers:
            log.warning(
                f'domain: {self.fetcher.domain}, '
                f'offers: {function.__name__}: {processed_offers}, '
                f'offers total for {function.__name__}: {total_offers}',
            )


    def generate_error_report(self):
        """Generate csv report with offer import errors."""
        self.error_report_generator.generate_report()


    def get_required_attributes_report_data(self) -> List[Dict[str, Any]]:
        """Get required attributes report data."""
        params_manager = self.offer_importer.params_manager
        result = []

        for info in self.generate_meta_offers():
            category_result = {
                'feed_category_id': info.feed_category_id,
                'market_category_id': info.ozon_category_id,
                'offers': [],
            }
            for offer_params in params_manager.collect_offer_import_params(
                    offers=info.offers,
                    feed_category_id=info.feed_category_id,
            ):
                offers_data = category_result['offers']
                offers_data.append({
                    'id': offer_params['offer_id'],
                    'name': offer_params['name'],
                    'attributes_errors': {
                        offer_params['ozon_attributes_data'][
                            source_id
                        ]['name']: err
                        for source_id, err
                        in offer_params['attributes_errors'].items()
                    },
                    'tags_errors': offer_params['tags_errors'],
                })
            result.append(category_result)

        return result


    def fetch_ozon_offer_categories(self):
        """Fetch all ozon offers description_category_id and type_id."""
        statistics = {
            'error': None,
            'domain': self.fetcher.domain,
        }
        try:
            statistics['processed_offers_num'] = self.fetcher.fetch_ozon_offer_categories()

        except Exception as err:
            err_msg = (
                f'[ERROR FETCHING OFFER CATEGORIES FOR DOMAIN: {self.fetcher.domain}]'
                f'[{err}]'
            )

            log.error(err_msg, exc_info=True)
            statistics['error'] = err_msg
            pprint(statistics)
            raise

        pprint(statistics)
