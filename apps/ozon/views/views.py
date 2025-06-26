"""Views for ozon."""
import re
import requests
from apps.mapper.permissions import IsStaffOrAdmin
from apps.ozon.exceptions import OzonProcessingException
from apps.ozon.models import OzonAuthKey, OzonFeedUrl
from apps.ozon.reports.errors_report_processor import OzonOffersErrorsReport
from apps.ozon.reports.scripts.errors_report_to_email import (
    run_errors_report_maker_detached,
)
from apps.ozon.serializers import (
    AuthDataSerializer,
    AuthKeySerializer,
    FeedUrlSerializer,
)
from rest_framework import status
from rest_framework.generics import GenericAPIView, ListAPIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework.viewsets import ModelViewSet


class AuthKeyViewSet(ModelViewSet):
    """Endpoint for auth keys."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = AuthKeySerializer
    queryset = OzonAuthKey.objects.all()


class FeedUrlViewSet(ModelViewSet):
    """Endpoint for feed urls."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = FeedUrlSerializer
    queryset = OzonFeedUrl.objects.all()


class OzonOffersErrorsReportView(APIView):
    """Endpoint for mapper reports."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get(
        self, request: Request, domain_id: str,
    ) -> Response:
        """Build mapper report and return url."""
        try:
            report_url = OzonOffersErrorsReport(domain_id).build_report()
            return Response(
                data={
                    'report_url': report_url,
                },
                status=status.HTTP_200_OK,
            )
        except OzonProcessingException as err:
            return Response(
                data={'error': str(err)},
                status=status.HTTP_404_NOT_FOUND,
            )
        except Exception as err:
            return Response(
                data={'error': str(err)},
                status=status.HTTP_404_NOT_FOUND,
            )

    def post(
        self, request: Request, domain_id: str,
    ) -> Response:
        """Start errors report generation in a subprocess."""
        try:
            emails = getattr(request.data, 'get', {}.get)('email')
            if not emails or type(emails) is not str:
                raise Exception('No emails provided')

            valid_emails = [
                email for email in emails.split(' ')
                if is_email_valid(email)
            ]
            if not valid_emails:
                raise Exception(
                    f'All provided emails are invalid. {emails.split(" ")}',
                )

        except Exception as err:
            return Response(
                data={'message': f'Error: {err}'},
                status=status.HTTP_400_BAD_REQUEST,
            )

        try:
            run_errors_report_maker_detached(domain_id, valid_emails)
            return Response(
                status=status.HTTP_202_ACCEPTED,
                data={'message': 'report generation started'},
            )

        except Exception as err:
            return Response(
                data={
                    'message': (
                        f'Report generation not started. Error: {err}'
                    ),
                },
                status=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )


def is_email_valid(email: str) -> bool:
    """Check if email str is valid."""
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    if (re.fullmatch(regex, email)):
        return True
    return False


class DomainViewSet(ListAPIView):
    """Show all Ozon domains."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)

    def get_queryset(self):
        """DB query set."""
        return sorted(OzonAuthKey.objects.values_list('domain', flat=True))

    def get(self, request):
        """REST get method."""
        return Response(self.get_queryset())


class AuthDataAPIView(GenericAPIView):
    """Endpoint for Ozon auth data validation."""

    permission_classes = (IsAuthenticated, IsStaffOrAdmin)
    serializer_class = AuthDataSerializer

    def post(self, request: Request, *args, **kwargs) -> Response:
        """Check if Ozon auth data is valid."""
        serializer_class = self.get_serializer_class()
        serializer = serializer_class(
            data=request.data,
            context={'request': request},
        )

        if not serializer.is_valid():

            return Response(
                {'detail': serializer.errors},
                status=status.HTTP_400_BAD_REQUEST,
            )

        client_id = request.data.get('client_id')  # type: ignore
        api_key = request.data.get('api_key')  # type: ignore

        url = 'https://api-seller.ozon.ru/v3/product/info/list'

        payload = {"offer_id": ["spam ham beans"]}
        headers = {
            'Host': 'api-seller.ozon.ru',
            'Content-Type': 'application/json',
            'Client-Id': client_id,
            'Api-Key': api_key,
        }

        response = requests.request('POST', url, headers=headers, data=payload)

        valid = False if response.status_code in [403, 401] else True

        data = {'valid': valid}

        return Response(data)
