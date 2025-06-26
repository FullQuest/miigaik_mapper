"""Celery beat jobs."""

from pathlib import Path

from celery import (
    shared_task,
    chain,
)
from apps.utils.scripts.run_parser import run_parser


@shared_task
def run_first_mapper_feed_parser():
    """Celery task for first feed parsing."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('parsing')
    script = 'first_mapper_feed_parser.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_mapper_feed_parser():
    """Celery task for feed parsing."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('parsing')
    script = 'mapper_feed_parser.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_feed_categories_fetcher():
    """Celery task for feed categories fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('feed')
    script = 'feed_categories_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_feed_attributes_fetcher():
    """Celery task for feed attributes fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('feed')
    script = 'feed_attributes_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_ozon_categories_fetcher():
    """Celery task for Ozon categories fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('ozon')
    script = 'ozon_categories_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_ozon_attributes_fetcher():
    """Celery task for Ozon attributes fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('ozon')
    script = 'ozon_attributes_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_ozon_values_fetcher():
    """Celery task for Ozon values fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('ozon')
    script = 'ozon_values_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_wildberries_categories_fetcher():
    """Celery task for Wildberries categories fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('wildberries')
    script = 'wildberries_categories_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_wildberries_attributes_fetcher():
    """Celery task for Wildberries attributes fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('wildberries')
    script = 'wildberries_attributes_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_wildberries_values_fetcher():
    """Celery task for Wildberries values fetcher."""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('fetchers') / Path('wildberries')
    script = 'wildberries_values_fetcher.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_mapper_report_to_mail_automatic():
    """Celery task for automatic mapper report"""
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('reports') / Path('scripts')
    script = 'mapper_report_automatic.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_garbage_collector_prepare_objects():
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('scripts') / Path('garbage_collector')
    script = 'prepare_objects_for_delete.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_garbage_collector_delete_objects():
    current_dir = Path(__file__).parent
    scripts_dir = current_dir / Path('scripts') / Path('garbage_collector')
    script = 'delete_outdated_objects.py'
    return run_parser(scripts_dir.as_posix(), script)


@shared_task
def run_garbage_collector_chain():
    chain_result = chain(
        run_garbage_collector_prepare_objects.s(),
        run_garbage_collector_delete_objects.si(),
    ).apply_async()
    return f"Chain started! Task ID: {chain_result.id}"

