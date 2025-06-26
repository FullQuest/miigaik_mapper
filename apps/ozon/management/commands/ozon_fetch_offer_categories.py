"""Fetch offer categories from OZON."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Ozon command."""

    method = 'fetch_ozon_offer_categories'
    filter_args = {'enable_posting': True}

    def add_arguments(self, parser):
        """Add command arguments."""
        parser.add_argument(
            '-d',
            '--domains',
            nargs='+',
            action='store',
            help='Domain names for processing',
            type=str,
        )
