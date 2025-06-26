"""Ozon status checker."""

import logging

from typing import Any, Dict, List, Optional

from apps.ozon.utils.api_connector.seller.api_wrapper import (
    get_product_import_info,
)

from .params_manager import ParamsConstructor

log = logging.getLogger('ozon_status_checker')

IMPORTED = 'imported'
FAILED = 'failed'


class OzonImportStatusChecker(ParamsConstructor):
    """Class for offer import status checking."""

    def check_import_status(
        self,
        task_ids: Optional[List[int]] = None
    ) -> Dict[str, List[dict]]:
        """Check and update offer import status.

        :param Optional[List[int]] task_ids: custom task_ids to process.

        Sets product ID if offer is successfully imported
        """
        import_product_infos: List[Dict[str, Any]] = []

        if task_ids is None:
            task_ids = self.fetcher.unprocessed_tasks_ids

        for task_id in task_ids:
            import_product_infos.extend(
                get_product_import_info(
                    domain=self.fetcher.domain,
                    task_id=task_id,
                ),
            )

        processed: List[Dict[str, Any]] = []
        not_processed: List[Dict[str, Any]] = []

        for import_product_info in import_product_infos:
            feed_offer_id = import_product_info['offer_id']
            status = import_product_info['status']

            if status == IMPORTED:
                self.fetcher.set_ozon_import_status(
                    feed_offer_id=feed_offer_id,
                    is_imported=True,
                )

                self.fetcher.set_ozon_product_id(
                    feed_offer_id=feed_offer_id,
                    product_id=import_product_info['product_id'],
                )

                processed.append(import_product_info)

            else:
                self.fetcher.set_ozon_import_status(
                    feed_offer_id=feed_offer_id,
                    is_imported=False,
                    clear_hash=status == FAILED,
                )

                not_processed.append(import_product_info)

        result = {
            'processed': processed,
            'not_processed': not_processed,
        }

        log.info(result)

        return result
