import logging
from functools import partial
from multiprocessing import Pool
from typing import Union

from apps.ozon.exceptions import MongoReverseException
from apps.ozon.library import OzonManageOffers
from apps.ozon.models import OzonAuthKey
from django.core.management.base import BaseCommand

log = logging.getLogger('ozon_manage_offers')


def manage_domain(
    domain: str,
    method: str,
    force: Union[bool, None] = None,
    **kwargs,
):
    """Manage domain processes."""
    try:
        if force:
            getattr(OzonManageOffers(domain, **kwargs), method)(force)

        getattr(OzonManageOffers(domain, **kwargs), method)()
    except MongoReverseException as err:
        log.error(
            f'No Mongo reverse for {domain} in {method} with: {err}',
        )
    except Exception as err:
        log.error(
            f'Unexpected exception for {domain} in {method} with: {err}',
        )


class OzonCommand(BaseCommand):
    """Command for for updating and posting offers to Ozon."""

    method: str
    filter_args: dict

    def add_arguments(self, parser):
        """Add arguments."""
        parser.add_argument(
            '-d',
            '--domains',
            nargs='+',
            action='store',
            help='Domain names for processing',
            type=str,
        )

    def handle(self, *args, **options):
        """Handle commands."""
        domains = options['domains']
        force = options.get('force')

        if not domains:
            domains = OzonAuthKey.objects.filter(
                **self.filter_args,
            ).values_list('domain', flat=True)

        partial_domain = partial(
            manage_domain,
            method=self.method,
            force=force,
        )

        with Pool(5) as pool:
            pool.map(partial_domain, domains)
