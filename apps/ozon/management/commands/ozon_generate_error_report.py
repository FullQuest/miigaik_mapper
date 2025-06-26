"""Import offers to Ozon."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Command for import and updating offers to Ozon."""
    method = 'generate_error_report'
    filter_args = {'is_disabled': False}

    def add_arguments(self, parser):
        parser.add_argument(
            '-d',
            '--domains',
            nargs='+',
            action='store',
            help='Domain names for processing',
            type=str
        )