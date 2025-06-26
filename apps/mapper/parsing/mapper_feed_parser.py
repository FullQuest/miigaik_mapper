"""Mapper feed parser."""

import logging
import os
import sys
from multiprocessing import Pool
from pprint import pprint
from typing import Any, Dict, List, Tuple

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


def mapper_feed_parser(data: MapperData):
    """Worker for parser.

    :param tuple data:
    """
    search, preset_name = data
    worker = FeedParserWorker(
        mysql_table_name=MYSQL_TABLE_NAME,
        mongo_db_name=MONGO_DATABASE_NAME,
        save_path='mapper_cached_feeds',
        mandatory_tags=['@id', 'categoryId', 'price'],
    )

    return worker.process_parser(
        search,
        preset_collection_name=preset_name,
    )


def mapper_feed_parser_main() -> bool:
    """
    ""Start parser.

    Parser for Ozon feeds, that was already parsed by first_mapper_feed_parser.

    This parser runs every couple hours and after feed updates.
    Parser is creating multiple processes after it fetches feed for
    for parsing from db.

    ДЛЯ КОМИССИИ: Этот участок кода был реализован в соответствии с внутренними
    правилами парсинга фидов. Именно по этому синхронизация с MySQL происходит
    через SQLAlchemy, а не через django ORM
    """
    table_name = 'mapper_feedmeta'

    engine, session = mysql_session()
    feed_urls = reflect_table(engine, table_name)

    query = session.query(feed_urls).filter_by(parsed=True, updated=False)

    feeds = query.all()

    if feeds == []:
        session.query(feed_urls).update({'updated': False})
        session.commit()
        feeds = query.all()

    feed_data: FeedsData = []

    for row in feeds:
        preset = f'feed_{row.id}'
        feed_data.append(({'id': row.id}, preset))

    session.close()
    engine.dispose()

    with Pool(processes=5) as pool:
        results = pool.map(mapper_feed_parser, feed_data)
        statistics = get_statistics(results)

    pprint(statistics)

    return True


if __name__ == '__main__':

    with run_checker('MapperFeedParserPython'):
        setproctitle('MapperFeedParserPython')
        mapper_feed_parser_main()
