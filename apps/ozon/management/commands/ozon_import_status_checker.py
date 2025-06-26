"""Check offer import status."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Command for checking offer import statuses."""
    method = 'check_import_status'
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
