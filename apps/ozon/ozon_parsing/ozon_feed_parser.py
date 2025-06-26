"""Main feed parser for Ozon."""

import logging
import os
import sys
from multiprocessing import Pool
from pprint import pprint

import django
from setproctitle import setproctitle

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from apps.utils import (FeedParserWorker, get_statistics, mysql_session,
                        reflect_table, run_checker)


log = logging.getLogger(__name__)

# FIXME reconsider mandatory tags for ozon
MANDATORY_TAGS = ['@id', 'price', 'categoryId']


def ozon_parse_feed(search: dict):
    """Worker for parser.

    :param dict search:
    """
    worker = FeedParserWorker(mysql_table_name='ozon_ozonfeedurl',
                              mongo_db_name='ozon',
                              save_path='ozon_cached_feeds',
                              mandatory_tags=MANDATORY_TAGS)

    return worker.process_parser(search,
                                 preset_collection_name=search['domain_id'])


def ozon_parser_main() -> bool:
    """Start parser.

    Parser for Ozon feeds, that was already parsed by ozon_first_feed_parser.

    This parser runs every couple hours and after feed updates.
    Parser is creating multiple processes after it fetches feed for
    for parsing from db.

    ДЛЯ КОМИССИИ: Этот участок кода был реализован в соответствии с внутренними
    правилами парсинга фидов. Именно по этому синхронизация с MySQL происходит
    через SQLAlchemy, а не через django ORM
    """
    table_name = 'ozon_ozonfeedurl'

    engine, session = mysql_session()
    feed_urls = reflect_table(engine, table_name)

    result = session.query(feed_urls).filter_by(parsed=True, updated=False)

    feeds = result.all()

    if feeds == []:
        session.query(feed_urls).update({'updated': False})
        session.commit()
        feeds = result.all()

    feeds_domains: list = []

    for row in feeds:
        feeds_domains.append({'domain_id': row.domain_id})

    session.close()
    engine.dispose()
    statistics: dict = {}

    with Pool(processes=5) as pool:
        results = pool.map(ozon_parse_feed, feeds_domains)
        statistics = get_statistics(results)

    pprint(statistics)

    return True


if __name__ == '__main__':
    run_checker('OzonFeedParserPython')
    setproctitle('OzonFeedParserPython')
    ozon_parser_main()
