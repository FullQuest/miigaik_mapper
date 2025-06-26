"""Module to make feed mapping report and send it to email."""
import os
import sys
import django
import logging
import argparse
import traceback
import subprocess

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from typing import List                     # noqa: E402
from datetime import datetime               # noqa: E402
from urllib.parse import urljoin            # noqa: E402

from django.core.mail import (              # noqa: E402
    EmailMessage,
)
from django.conf import (              # noqa: E402
    settings,
)
from apps.mapper.reports.reports import (   # noqa: E402
    FeedMapperReport,
)

BASE_URL = 'http://hub.b2basket.ru'
SCRIPT_NAME = __file__
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

log = logging.getLogger(__name__)


def run_mapper_report_maker_detached(
    feed_id: int,
    emails: List[str],
):
    """Run mapper report maker in a subprocess.

    :param int feed_id: mapper feed id
    :param List[str] emails: emails list to send report to
    """
    command = (
        f'cd {SCRIPT_PATH} && {settings.PYTHON_PATH} {SCRIPT_NAME} '
        f'--feed-id {feed_id} --emails {" ".join(emails)}',
    )
    subprocess.Popen(command, shell=True)


def make_report(
    feed_id: int,
    emails: List[str],
    marketplace_id: int = 1,
):
    """Make mapping report for given feed and send to provided emails.

    :param int feed_id: mapper feed id
    :param List[str] emails: emails list to send report to
    :param marketplace_id int: mapper marketplace id
    """
    current_date = datetime.now().strftime("%d-%m-%Y")

    try:
        report_path = FeedMapperReport(marketplace_id, feed_id).build_report()
    except Exception as err:
        tb = traceback.format_exc()
        message = EmailMessage(
            subject=f'Mapping report error [{current_date}]',
            body=f'Err info: {err}: {tb}',
            to=emails,
        )
        message.send()
        return

    report_url = urljoin(BASE_URL, report_path)

    email_subject = f'Mapping report for feed id {feed_id} [{current_date}]'
    email_body = f'The report is available at: {report_url}'

    message = EmailMessage(
        subject=email_subject,
        body=email_body,
        to=emails,
    )
    message.send()


def parse_args(args: List[str]):
    """Parse cmd-line arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '-f',
        '--feed-id',
        help='Mapper feed id to make report for',
        action='store',
        required=True,
        type=int,
    )
    parser.add_argument(
        '-e',
        '--emails',
        help="Emails list. Example: --emails foo@bar.com bar@foo.com",
        nargs='+',
        action="store",
        required=True,
    )
    parser.add_argument(
        '-s',
        '--subprocess',
        help='Run report maker in a subprocess.',
        action='store_true',
        default=False,
    )
    return parser.parse_args(args)


if __name__ == '__main__':
    import os
    import sys
    import django
    from setproctitle import setproctitle

    sys.path.append('/home/server/b2basket/')
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
    django.setup()

    script_name = 'MappingReportToEmail'

    from apps.utils import run_checker

    args = parse_args(sys.argv[1:])

    feed_id = args.feed_id
    emails = args.emails
    run_in_subprocess = args.subprocess

    if type(emails) is str:
        emails = [emails]

    with run_checker(script_name):
        setproctitle(script_name)

        if run_in_subprocess:
            run_mapper_report_maker_detached(feed_id, emails)
        else:
            make_report(feed_id, emails)
