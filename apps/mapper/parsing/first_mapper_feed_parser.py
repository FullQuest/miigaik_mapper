"""First mapper feed parser."""

import logging
import os
import sys
from multiprocessing import Pool
from pprint import pprint
from typing import Dict, List, Tuple

import django
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.utils import (
    FeedParserWorker,
    get_statistics,
    mysql_session,
    reflect_table,
    run_checker,
)

log = logging.getLogger(__name__)

MYSQL_TABLE_NAME = 'mapper_feedmeta'
MONGO_DATABASE_NAME = 'mapper'

MapperData = Tuple[Dict[str, str], str]
FeedsData = List[Tuple[Dict[str, str], str]]


def first_mapper_feed_parser(data: MapperData):
    """Worker for first parser.

    :param tuple data:
    """
    search, preset_name = data
    worker = FeedParserWorker(
        mysql_table_name=MYSQL_TABLE_NAME,
        mongo_db_name=MONGO_DATABASE_NAME,
        first=True,
        save_path='mapper_cached_feeds',
        mandatory_tags=['@id', 'categoryId', 'price'],
    )

    return worker.process_parser(
        search,
        preset_collection_name=preset_name,
    )


def first_mapper_feed_parser_main() -> bool:
    """
    Parser only for first time uploaded mapper feeds.

    When feed is uploaded by user, field "parsed" in table feed_urls is
    set to False. This parser is running very often by Celery, so user can
    quickly start to edit feed. If feed is succesfully parsed,
    "parsed" will be set to True. Parser is creating multiple processes
    after ir fetches feed for parsing from DB.

    ДЛЯ КОМИССИИ: Этот участок кода был реализован в соответствии с внутренними
    правилами парсинга фидов. Именно по этому синхронизация с MySQL происходит
    через SQLAlchemy, а не через django ORM
    """
    engine, session = mysql_session()
    feed_urls = reflect_table(engine, MYSQL_TABLE_NAME)

    query = session.query(feed_urls).filter_by(parsed=False, deleted=False)

    feeds = query.all()

    feed_data: FeedsData = []

    for row in feeds:
        preset = f'feed_{row.id}'
        feed_data.append(({'id': row.id}, preset))

    session.close()
    engine.dispose()

    with Pool(processes=5) as pool:
        results = pool.map(first_mapper_feed_parser, feed_data)
        statistics = get_statistics(results)

    pprint(statistics)

    return True


if __name__ == '__main__':

    with run_checker('MapperFirstFeedParserPython'):
        setproctitle('MapperFirstFeedParserPython')
        first_mapper_feed_parser_main()
