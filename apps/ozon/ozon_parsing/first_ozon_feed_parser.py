"""Parser for newly added feeds."""

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

# FIXME RECONSIDER MANDATORY TAGS FOR OZON
MANDATORY_TAGS = ['@id', 'price', 'categoryId']


def first_ozon_parse_feed(search: dict):
    """Worker for first parser.

    :param dict search:
    """
    worker = FeedParserWorker(mysql_table_name='ozon_ozonfeedurl',
                              mongo_db_name='ozon',
                              first=True,
                              save_path='ozon_cached_feeds',
                              mandatory_tags=MANDATORY_TAGS)

    return worker.process_parser(search,
                                 preset_collection_name=search['domain_id'])


def first_ozon_parser_main() -> int:
    """Parser only for first time uploaded Ozon feeds

    When feed is uploaded by user, field "is_parsed" in table feed_urls is
    set to False. This parser is running very often by Celery, so user can
    quickly start to edit feed. If feed is succesfully parsed,
    "is_parsed" will be set to True. Parser is creating multiple processes
    after ir fetches feed for parsing from DB.

    ДЛЯ КОМИССИИ: Этот участок кода был реализован в соответствии с внутренними
    правилами парсинга фидов. Именно по этому синхронизация с MySQL происходит
    через SQLAlchemy, а не через django ORM
    """
    table_name = 'ozon_ozonfeedurl'

    engine, session = mysql_session()
    feed_urls = reflect_table(engine, table_name)
    result = session.query(feed_urls).filter_by(parsed=False, deleted=False)

    feeds = result.all()

    feeds_domains: list = []

    for row in feeds:
        feeds_domains.append({'domain_id': row.domain_id})

    session.close()
    engine.dispose()

    statistics: dict = {}

    with Pool(processes=5) as pool:
        results = pool.map(first_ozon_parse_feed, feeds_domains)
        statistics = get_statistics(results)

    pprint(statistics)

    return 1


if __name__ == '__main__':

    run_checker('OzonFirstFeedParserPython')
    setproctitle('OzonFirstFeedParserPython')
    first_ozon_parser_main()
