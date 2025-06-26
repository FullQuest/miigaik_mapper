"""Fetch offers data from database."""
import logging

from typing import Any, Dict, List, Optional, Set, Union, Tuple
from pymongo.collection import Collection

from django.utils import timezone
from django.utils.functional import cached_property

from apps.ozon.models import (
    OzonAuthKey,
    OzonFeedUrl,
    OzonOffer,
)
from apps.utils import (
    MongoConnMixin,
    map_category_id_name,
    mongo_find_result_checker,
)
from apps.mapper.utils.utils import (
    get_category_map,
    get_category_attribute_map,
    get_market_category_attributes,
)
from apps.ozon.utils.api_connector.seller.api_wrapper import (
    get_all_products_attribute_info,
)

log = logging.getLogger(__name__)

FAILED_STATES = [
    'failed_moderation',
    'failed_validation',
    'failed',
]

OZON = 'ozon'


class FetchOzonOfferData(MongoConnMixin):
    """Class for fetching data from MongoDB to post offers to Ozon.

    When initialized creates connection to MongoDB with
    required collection names and attributes.

    Related Mixin :class:`~apps.utils.mongo_utils.MongoConnMixin`

    Initial values:

    :param str domain: Domain name in system
    :param offers: Offers
    :param feed_categories: Categories

    :param dict category_map: Fetched category maps
    :param list feed_categories_ids: Fetched feed categories
    :param dict feed_categories_names: Category ID names
    :param dict fetched_ozon_products_ids: Fetched Ozon product ID`s
    :param dict fetched_sql_offers: Fetched offers from sql
    :param set unprocessed_tasks_ids: Unprocessed product import task ID`s
    :param list unprocessed_offers_ids: Unprocessed offers ID's
    :param list processed_offers_ids: Processed offers ID's
    :param list failed_import_offer_ids: Failed product import offer ID`s
    :param list imported_offers_errors: Imported offers error data
    :param list ozon_warehouse_ids: Fetched ozon warehouses ID's for domain
    :param dict warehouse_map: Fetched warehouse map for domain
    :param dict domain_settings: Ozon Domain settings
    """

    def __init__(self, dbname, collection, **kwargs):
        """Start initial preparations."""
        super().__init__(dbname, collection, **kwargs)

        self.domain = collection

        self.feed_categories: Collection = getattr(self.db, f'{self.domain}.categories')            # noqa: E501
        self.offers: Collection = getattr(self.db, f'{self.domain}.offers')                         # noqa: E501
        self.offers_data: Collection = getattr(self.db, f'{self.domain}.offers_data')               # noqa: E501
        self.last_stocks: Collection = getattr(self.db, f'{self.domain}.last_stocks')               # noqa: E501
        self.last_stocks_info: Collection = getattr(self.db, f'{self.domain}.last_stocks_info')     # noqa: E501
        self.custom_mapping_domain: str = getattr(
            OzonAuthKey.objects.get(domain=self.domain).mapping_from_domain,
            'domain',
            self.domain,
        )

        self.category_map = self._get_domain_category_map(self.custom_mapping_domain)
        self.feed_categories_ids = self.get_feed_categories_ids()
        self.feed_categories_names = self.get_feed_categories_names()
        self.fetched_ozon_products_ids = self.get_ozon_products_ids()
        self.unprocessed_tasks_ids = self.get_unprocessed_task_ids()
        self.unprocessed_offers_ids = self.get_unprocessed_offers_ids()
        self.processed_offers_ids = self.get_processed_offers_ids()
        self.failed_import_offer_ids = self.get_failed_import_offer_ids()
        self.imported_offers_errors = self.get_imported_offers_errors()
        self.domain_settings = self.get_domain_settings()
        self.feed_params = self.get_feed_params()

    def _get_domain_category_map(
        self,
        custom_domain: Optional[str] = None
    ) -> Dict[int, Dict[str, int]]:
        """Get category map from mapper for self.domain.

        :param str custom_domain: OzonAuthKey used for domains, that works
                                  with another domain mapping
        """
        return get_category_map(custom_domain or self.domain, OZON)

    def get_ozon_offer_categories(
        self,
        offer_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        get_attr_kwargs = {'domain': self.domain}

        if offer_ids:
            get_attr_kwargs['offer_id'] = offer_ids

        return [
            {
                'offer_id': offer['offer_id'],
                'description_category_id': offer['description_category_id'],
                'type_id': offer['type_id'],
            }
            for offer in get_all_products_attribute_info(**get_attr_kwargs)
        ]

    def fetch_ozon_offer_categories(
        self,
        offer_ids: Optional[List[str]] = None,
    ) -> int:

        fetched_offer_categories = self.get_ozon_offer_categories(offer_ids)

        collection: Collection = getattr(self.db, f'{self.domain}.offer_categories')

        if offer_ids:
            collection.delete_many({'offer_id': {'$in': offer_ids}})
        else:
            collection.drop()

        collection.insert_many(fetched_offer_categories)

        return len(fetched_offer_categories)

    @cached_property
    def ozon_category_info(self) -> Dict[str, Dict[str, int]]:
        """Fetch all ozon offers description_category_id and type_id.

        :return Dict[str, Dict[str, int]]: Ozon offer categories info
            Key is offer_id, value dict is offer category\type info. Example:
            {
                323153: {
                    "description_category_id": 5123,
                    "type_id": 321,
                },
            }
        """
        collection: Collection = getattr(self.db, f'{self.domain}.offer_categories')

        return {
            offer.pop('offer_id'): offer
            for offer in collection.find({}, {"_id": 0})
        }

    def get_category_attribute_map(
        self,
        category_mapping_id: int,
    ) -> Dict[str, str]:
        """Fetch category attribute map from mapper.

        :return: attribute_map
        :rtype: Dict[str, str]  FIXME:
        """
        return get_category_attribute_map(category_mapping_id)

    @staticmethod
    def get_ozon_category_attributes(
        ozon_category_id: str,
    ) -> Dict[str, Dict[str, Any]]:
        """Fetch ozon category attributes data."""
        return get_market_category_attributes(
            marketplace=OZON,
            category_source_id=ozon_category_id,
        )

    @mongo_find_result_checker
    def get_feed_categories_ids(self) -> List[str]:
        """Fetch categoriees ids from MongoDB collection for self.domain.

        Decorated with:
        :meth: `~apps.utils.mongo_utils.mongo_find_result_checker`

        Example of feed_categories_ids:
            ['424242', '123123', '01010101']

        :return: feed_categories_ids
        :rtype: List[str]
        """
        fetched_feed_categories_ids = list(
            self.feed_categories.find({}, {'_id': 0, '@id': 1}),
        )

        feed_categories_ids = [
            category['@id'] for category in fetched_feed_categories_ids
        ]

        return feed_categories_ids

    @mongo_find_result_checker
    def get_feed_categories_names(self) -> Dict[str, str]:
        """Fetch feed categories with names from MongoDB collection.
        Example of categories_ids_names:

            {'123123: 'Spam', '424242': 'Ham'}

        :return: feed_categories_names
        :rtype: Dict[str, str]
        """
        feed_categories = list(self.feed_categories.find({}, {'_id': 0}))

        feed_categories_names = map_category_id_name(feed_categories)

        return feed_categories_names

    @mongo_find_result_checker
    def get_feed_offers(self, category_id) -> List[Dict[str, Any]]:
        """Fetch feed offers from offers MongoDB collection for given category.

        :return: feed_offers
        :rtype: List[Dict[str, Any]]
        """
        feed_offers = list(
            self.offers.find({'categoryId': category_id}, {'_id': 0}),
        )

        return feed_offers

    @mongo_find_result_checker
    def get_all_feed_offers(self) -> List[Dict[str, Any]]:
        """Fetch all feed offers from offers MongoDB collection.

        :return: feed_offers
        :rtype: List[Dict[str, Any]]
        """
        feed_offers = list(self.offers.find({}, {'_id': 0}))

        return feed_offers

    def get_ozon_products_ids(self) -> Dict[str, int]:
        """Fetch posted Ozon offers from MySQL.

        :return: Ozon products ids
        :rtype: Dict[str, int]
        """
        ozon_offers = OzonOffer.objects.filter(
            domain=self.domain,
            is_imported=True,
        ).exclude(product_id=None)

        if ozon_offers:
            return {
                offer.feed_offer_id: offer.product_id
                for offer in ozon_offers
            }
        else:
            return {}


    def get_unprocessed_offers_ids(self) -> List[str]:
        """Fetch imported unprocessed offers ID's.

        :return unprocessed_offers_ids: Imported but not processed offer's ID's
        :rtype: List[str]
        """
        unprocessed_offers = OzonOffer.objects.all().filter(
            domain=self.domain,
            is_processed=False,
            is_imported=True,
        )

        unprocessed_offers_ids = [
            offer.feed_offer_id
            for offer in unprocessed_offers
        ]

        return unprocessed_offers_ids

    def get_processed_offers_ids(self) -> List[str]:
        """Fetch imported and processed offers ID's.

        :return processed_offers_ids: Imported and processed offers
        :rtype: List[str]
        """
        processed_offers = OzonOffer.objects.all().filter(
            domain=self.domain,
            is_processed=True,
            is_imported=True,
        )

        processed_offers_ids = [
            offer.feed_offer_id
            for offer in processed_offers
        ]

        return processed_offers_ids

    def get_unprocessed_task_ids(self) -> Set[int]:
        """Fetch unprocessed offers import task ID`s.

        :return: Unprocessed offers task ID`s
        :rtype: Set[int]
        """
        unprocessed_offers = OzonOffer.objects.all().filter(
            domain=self.domain,
            is_imported=False,
        ).exclude(is_processed=True)

        unprocessed_offers_import_task_ids = {
            offer.task_id
            for offer in unprocessed_offers
        }

        return unprocessed_offers_import_task_ids

    def get_failed_import_offer_ids(self) -> List[str]:
        """Get offer ID`s for failed product imports."""
        failed_offers = OzonOffer.objects.all().filter(
            domain=self.domain,
            state__in=FAILED_STATES,
        )

        failed_import_offer_ids = [
            offer.feed_offer_id
            for offer in failed_offers
        ]

        return failed_import_offer_ids

    def get_imported_offers_errors(self) -> List[Dict[str, str]]:
        """Get imported offers validation errors."""
        imported_offers = OzonOffer.objects.all().filter(
            domain=self.domain,
            is_processed=False,
            is_imported=True,
        ).exclude(errors=False)

        imported_offers_error_data = [
            {
                'feed_offer_id': offer.feed_offer_id,
                'errors': offer.errors,
            }
            for offer in imported_offers
        ]

        return imported_offers_error_data

    def set_ozon_offer_state(
        self,
        feed_offer_id: str,
        state: str,
        is_processed: Optional[bool] = False,
    ) -> OzonOffer:
        """Set Ozon offer state.

        :param str feed_offer_id: Feed offer ID
        :param str state: Current product status in the system
        :param bool is_processed: True if offer state == processed

        :return result: Operation result
        :rtype: OzonOffer
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        update_time_field = 'updated_at'

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=feed_offer_id,
            defaults={
                'state': state,
                'is_processed': is_processed,
            },
        )
        setattr(offer, update_time_field, timezone.now())
        offer.save()

        return offer

    def set_ozon_error_description(
        self,
        feed_offer_id: str,
        error_description: Dict[str, Any],
    ) -> OzonOffer:
        """Set Ozon error description.

        :param str feed_offer_id: Feed offer ID
        :param error_description: Fetched Ozon error description

        :return result: Operation result
        :rtype: OzonOffer
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        update_time_field = 'updated_at'

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=feed_offer_id,
            defaults={
                'errors': error_description,
            },
        )
        setattr(offer, update_time_field, timezone.now())
        offer.save()

        return offer

    def set_ozon_import_status(
        self,
        feed_offer_id: str,
        is_imported: bool = False,
        clear_hash: bool = False,
    ) -> OzonOffer:
        """Set Ozon import status.

        :param str feed_offer_id: Feed offer ID
        :param bool is_imported: True if import status == imported
        :param bool clear_hash: True if need to clear import hash

        :return import_status_info: Operation result
        :rtype: Dict[str, Any]
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        field_values: Dict[str, Any] = {
            'is_imported': is_imported,
        }
        if clear_hash:
            field_values['last_import_hash'] = ''

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=feed_offer_id,
            defaults=field_values,
        )

        return offer

    def set_ozon_offer_start_import(
        self,
        feed_offer_id: str,
        task_id: int,
        import_hash: str,
    ) -> OzonOffer:
        """Set Ozon offer import task ID.

        :param str feed_offer_id: Feed offer ID
        :param int task_id: Code of product import task on Ozon
        :param str import_hash: Hash sum of imported parameters

        :return result: Operation result
        :rtype: Dict[str, Union[str, bool]]
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=feed_offer_id,
            defaults={
                'task_id': task_id,
                'last_import_hash': import_hash,
            },
        )

        return offer

    def set_ozon_product_id(
        self,
        feed_offer_id: str,
        product_id: int,
    ) -> OzonOffer:
        """Set Ozon product ID.

        :param str feed_offer_id: Feed offer ID
        :param int product_id: Ozon product ID

        :return result: Operation result
        :rtype: OzonOffer
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=feed_offer_id,
            defaults={
                'product_id': product_id,
            },
        )

        return offer


    def set_ozon_offer_start_update(
        self,
        offer_id: str,
        task_id: int,
        import_hash: str,
    ) -> OzonOffer:
        """Set offer flags for start update."""
        domain = OzonAuthKey.objects.get(domain=self.domain)

        offer, created = OzonOffer.objects.update_or_create(
            domain=domain,
            feed_offer_id=offer_id,
            defaults={
                'errors': '',
                'is_imported': False,
                'is_processed': False,
                'task_id': task_id,
                'last_import_hash': import_hash,
            },
        )

        return offer

    def set_ozon_update_date(
        self,
        feed_offer_id: int,
        errors: Optional[str] = None,
        update_target: Optional[str] = None,
    ) -> OzonOffer:
        """Set date of last offer update on Ozon.

        :param str feed_offer_id: Feed offer id
        :param str errors: Errors occurred while editing product on Ozon

        :return offer_id: Offer id in MySQL
        :rtype: OzonOffer
        """
        domain = OzonAuthKey.objects.get(domain=self.domain)

        if errors is None:
            offer, _ = OzonOffer.objects.update_or_create(
                domain=domain,
                feed_offer_id=feed_offer_id,
                defaults={
                    'errors': '',
                },
            )
            if update_target == 'stocks':
                offer.last_stocks_update = timezone.now()
            elif update_target == 'prices':
                offer.last_price_update = timezone.now()

            offer.save()

        else:
            if update_target:
                errors = f'There were {update_target} update errors: {errors}'
            offer, _ = OzonOffer.objects.update_or_create(
                domain=domain,
                feed_offer_id=feed_offer_id,
                defaults={
                    'errors': errors,
                },
            )

        return offer


    def get_ozon_auth(self):
        """Get domain object."""
        return OzonAuthKey.objects.get(domain=self.domain)

    def get_feed_url(self):
        """Get feed url object."""
        return OzonFeedUrl.objects.get(domain=self.domain)

    def get_feed_params(self) -> Dict[str, Any]:
        feed = self.get_feed_url()

        return {
            'feed_hash': feed.parsed_hash,
            'price_hash': feed.update_price_hash,
            'stock_hash': feed.update_stock_hash,
        }

    def get_domain_settings(self) -> Dict[str, Any]:
        """Get domain settings."""
        ozon_domain_settings = OzonAuthKey.objects.filter(
            domain=self.domain,
        ).first()

        domain_settings: Dict[str, Any] = {}

        if ozon_domain_settings:
            domain_settings['is_disabled'] = (
                ozon_domain_settings.is_disabled
            )
            domain_settings['full_update_allowed'] = (
                ozon_domain_settings.full_update_allowed
            )
            domain_settings['enable_posting'] = (
                ozon_domain_settings.enable_posting
            )

        return domain_settings

    @mongo_find_result_checker
    def get_fbs_sku(self) -> Dict[str, int]:
        """Get fbs sku by offer id."""
        return {
            offer['@id']: offer['fbs_sku']
            for offer in self.offers_data.find(
                {'fbs_sku': {'$gt': 0}},
                {'_id': 0},
            )
        }

    def get_last_update_stocks(
        self,
    ) -> Dict[Tuple[str, int], Dict[str, Union[int, str]]]:
        """Get last update stocks."""
        return {
            (stock['offer_id'], stock['warehouse_id']): {
                'stock': stock['stock'],
                'errors': stock.get('errors', ''),
            }
            for stock in self.last_stocks.find({}, {'_id': 0})
        }

    def set_last_update_stocks(self, stocks):
        """Set last update stocks."""
        self.last_stocks.drop()
        self.last_stocks.insert_many(stocks)

    def get_last_import_hash(self, offer_id: str) -> str:
        """Get last import hash."""
        domain = OzonAuthKey.objects.get(domain=self.domain)
        offer = OzonOffer.objects.get(domain=domain, feed_offer_id=offer_id)
        return offer.last_import_hash
