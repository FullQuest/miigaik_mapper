"""Celery beat jobs."""
import logging
import subprocess

from pathlib import Path
from celery import shared_task
from django.conf import settings

from apps.utils.scripts.run_parser import run_parser

log = logging.getLogger('ozon_tasks')


@shared_task
def run_first_ozon_feed_parser():
    """Celery task for first feed parsing."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('ozon_parsing')
    script = 'first_ozon_feed_parser.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_ozon_feed_parser():
    """Celery task for renew ozon offers."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('ozon_parsing')
    script = 'ozon_feed_parser.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def ozon_category_tree_fetcher():
    """Fetch category tree from Ozon."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('scripts')
    script = 'category_tree_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


def run_ozon_management_command(command: str):
    """Start management command in subprocess.

    :param str command: name of the management command which will be executed
    """
    proc = subprocess.Popen((f'{settings.PYTHON_PATH} manage.py '
                             f'{command}'),
                            shell=True,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (stdout, stderr) = proc.communicate()

    if proc.returncode:
        raise Exception(stderr.decode('utf-8').replace('\n', ' '))
    else:
        return stdout.decode('utf-8').replace('\n', ' ')


@shared_task
def ozon_fetch_offer_categories():
    """Process ozon orders."""
    command = f'ozon_fetch_offer_categories'
    return run_ozon_management_command(command)


@shared_task
def ozon_import_offers():
    """Import new offers to Ozon."""
    command = 'ozon_import_offers'
    return run_ozon_management_command(command)


@shared_task
def ozon_import_status_checker():
    """Check import status."""
    command = 'ozon_import_status_checker'
    return run_ozon_management_command(command)


@shared_task
def ozon_update_offers():
    """Update offers on Ozon."""
    command = 'ozon_update_offers'
    return run_ozon_management_command(command)
