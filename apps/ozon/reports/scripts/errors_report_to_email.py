"""Module to make Ozon offers errors report and send it to email."""
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

from typing import List                                     # noqa: E402
from datetime import datetime                               # noqa: E402
from urllib.parse import urljoin                            # noqa: E402

from django.core.mail import EmailMessage                   # noqa: E402
from django.conf import settings                            # noqa: E402
from apps.config.utils.config_get_value import get_value    # noqa: E402
from apps.ozon.reports.errors_report_processor import (     # noqa: E402
    OzonOffersErrorsReport,
)

BASE_URL = 'http://hub.b2basket.ru'
SCRIPT_NAME = __file__
SCRIPT_PATH = os.path.dirname(os.path.realpath(__file__))

log = logging.getLogger(__name__)


def run_errors_report_maker_detached(
    domain: str,
    emails: List[str],
):
    """Run mapper report maker in a subprocess.

    :param str domain: OzonAuthKey domain
    :param List[str] emails: emails list to send report to
    """
    command = (
        f'cd {SCRIPT_PATH} && {settings.PYTHON_PATH} {SCRIPT_NAME} '
        f'--domain {domain} --emails {" ".join(emails)}',
    )
    subprocess.Popen(command, shell=True)


def make_report(
    domain: str,
    emails: List[str],
):
    """Make ozon errors report for given domain and send to provided emails.

    :param str domain: OzonAuthKey domain
    :param List[str] emails: emails list to send report to
    """
    current_date = datetime.now().strftime("%d-%m-%Y")

    try:
        report_path = OzonOffersErrorsReport(domain).build_report()
    except Exception as err:
        tb = traceback.format_exc()
        EmailMessage(
            subject=f'Ozon errors report error {domain} [{current_date}]',
            body=(
                'В процессе подготовки отчета произошла ошибка. '
                'Обратитесь в техническую поддержку.'
            ),
            to=emails,
        ).send()

        admin_emails = get_value('admin_emails')
        if admin_emails:
            EmailMessage(
                subject=f'Ozon errors report error {domain} [{current_date}]',
                body=(
                    f'Receivers: {emails}\n'
                    f'Domain: {domain}\n'
                    f'Error: {err}\n'
                    f'Traceback: {tb}'
                ),
                to=admin_emails.split('\n'),
            ).send()
        return

    report_url = urljoin(BASE_URL, report_path)

    email_subject = f'Ozon errors report for domain {domain} [{current_date}]'
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
        '-d',
        '--domain',
        help='Ozon domain to make report for',
        action='store',
        required=True,
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

    script_name = 'OzonErrorsReportToEmail'

    from apps.utils import run_checker

    args = parse_args(sys.argv[1:])

    domain = args.domain
    emails = args.emails
    run_in_subprocess = args.subprocess

    if type(emails) is str:
        emails = [emails]

    with run_checker(script_name):
        setproctitle(script_name)

        if run_in_subprocess:
            run_errors_report_maker_detached(domain, emails)
        else:
            make_report(domain, emails)
