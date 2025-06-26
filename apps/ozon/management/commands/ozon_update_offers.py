"""Update posted offer on Ozon."""

from .base_command_executor import OzonCommand


class Command(OzonCommand):
    """Ozon command."""

    method = 'update_offers_on_ozon'
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

        parser.add_argument(
            '-f',
            '--force',
            action='store',
            help='Force update without checking last hash',
            default=False,
        )

    def handle(self, *args, **options):
        """Handle script options."""
        if options['force']:
            self.filter_args.update({'enable_daily_offers_update': True})

        super().handle(*args, **options)
