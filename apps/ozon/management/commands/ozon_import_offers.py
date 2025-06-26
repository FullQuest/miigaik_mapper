"""Import offers to Ozon."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Command for import and updating offers to Ozon."""
    method = 'import_offers_to_ozon'
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
