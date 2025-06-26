"""Fetch imported offer info."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Command for fetching imported offers info."""
    method = 'fetch_imported_offers_info'
    filter_args = {'enable_posting': True}

    def add_arguments(self, parser):
        parser.add_argument(
            '-d',
            '--domains',
            nargs='+',
            action='store',
            help='Domain names for processing',
            type=str
        )