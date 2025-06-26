"""Module to send report automatically on time"""
import os
import sys
import django

sys.path.append('/home/server/b2basket/')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'b2basket.settings')
django.setup()

from typing import List, Dict                                       # noqa: E402

from apps.mapper.reports.scripts.mapper_report_to_email import (    # noqa: E402
    make_report,
)
from apps.mapper.models import MapperAlertEmail                     # noqa: E402


def get_data_for_report() -> Dict[int, List[str]]:
    """Get feeds and email addresses for reports

    :return Dict[int, List[str]]: feed_id, list of emails
    """

    emails = MapperAlertEmail.objects.filter(
        get_mapping_report=True,
        feed__deleted=False,
    )

    emails_by_feed_id: Dict[int, List[str]] = {}
    for email in emails:
        emails_by_feed_id.setdefault(email.feed.id, []).append(email.email)

    return emails_by_feed_id


if __name__ == '__main__':
    from setproctitle import setproctitle

    script_name = 'MappingReportAutomatic'

    from apps.utils import run_checker

    with run_checker(script_name):
        setproctitle(script_name)

        data = get_data_for_report()
        for feed_id, emails in data.items():
            make_report(
                feed_id=feed_id,
                emails=emails,
            )
