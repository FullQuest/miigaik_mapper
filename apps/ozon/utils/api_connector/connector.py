"""Ozon API connector."""
import logging
import traceback
from json.decoder import JSONDecodeError
from typing import Any, Dict, List, NamedTuple, Optional, Union, cast

from apps.ozon.models import OzonAuthKey

from apps.ozon.utils.api_connector.seller.urls import (
    SANDBOX_SETTINGS,
    SANDBOX_URL,
    SELLER_API_URL,
)
from apps.utils.yandex_cloud.logging_utils.clickhouse.event_types import (
    OzonEvent,
)
from apps.utils.yandex_cloud.logging_utils.clickhouse.record_types import (
    BaseLogRecord,
)
from requests import PreparedRequest, Request, Session

log = logging.getLogger('ozon_api_connector')
clickhouse_logger = logging.getLogger('clickhouse_logger')

RequestConfig = Dict[str, Union[str, List[str]]]


class MetaDict(dict):
    """Dict with unlocked attributes."""

    metadata: Dict[str, Any]


class MetaList(list):
    """List with unlocked attributes."""

    metadata: Dict[str, Any]


class MethodMeta(NamedTuple):
    """Meta info class for url."""

    url: str
    request_type: str = 'POST'


class OzonConnector:
    """Base Ozon API connector."""

    def __init__(self, config: MethodMeta):
        """Initialize class according to provider params.

        :param dict config: Dict with configuration keys
        :param str base_url: Base url for request or BASE_URL by default
        """
        self.base_url = ''
        self.request_type = cast(str, config.request_type)
        self.url = cast(str, config.url)

    def build_request(
        self,
        params: Dict[str, Any],
        url_params: Optional[Dict[str, Any]] = None,
        query_params: Union[Dict[str, Any], None] = None,
        sandbox: bool = False,
    ) -> Request:  # type: ignore
        """Template method."""
        pass

    def inject_headers(
        self,
        domain: str,
        prepared_request: PreparedRequest,
        *args,
        **kwargs,
    ) -> PreparedRequest:  # type: ignore
        """Template method."""
        pass

    def request_to_ozon(
        self,
        domain: str,
        params: Dict[str, Any],
        url_params: Union[Dict[str, Any], None] = None,
        query_params: Union[Dict[str, Any], None] = None,
        trace_requests: bool = True,
        sandbox: bool = False,
    ) -> Union[dict, Any]:
        """Make plain request to Ozon and return response.

        :param str domain: Domain name in system
        :param url_params: url params
        :param query_params: query params

        :param params: Params for JSON data
        :type params: Dict[str, Any]

        :param bool trace_requests: Option to log request and response
        :param bool sandbox: Option for debugging

        :return: Response from Ozon API
        :rtype: dict
        """
        request = self.build_request(
            params=params,
            sandbox=sandbox,
            url_params=url_params,
            query_params=query_params,
        )

        session = Session()
        prepared_request = session.prepare_request(request)
        prepared_request = self.inject_headers(
            domain=domain,
            prepared_request=prepared_request,
            sandbox=sandbox,
        )
        metadata = {
            'request': {
                'url': str(prepared_request.url),
                'params': str(prepared_request.body),
                'headers': dict(prepared_request.headers),
            },
        }

        response = session.send(prepared_request)

        try:
            json_data = MetaDict(response.json())
            json_data.metadata = metadata

            error = response.status_code != 200

        except JSONDecodeError:
            error = True

            if response.headers.get('content-type') in [
                'application/pdf',
                'text/csv; charset=utf-8',
            ]:

                return response.content

            if trace_requests:
                metadata['response'] = {
                    'domain': domain,
                    'status': response.status_code,
                    'headers': response.headers,
                    'traceback': traceback.format_exc(),
                    'content': response.content,
                }
                log.error(metadata)

            json_data = None

        clickhouse_logger.info(
            BaseLogRecord(  # type: ignore
                app='ozon',
                event=OzonEvent.request_error if error else OzonEvent.request,
                level='ERROR' if error else 'INFO',
                domain=domain,
                request_to=prepared_request.headers.get('Host', ''),
                request_body=prepared_request.body,
                request_headers=f'{prepared_request.headers}',
                request_url=prepared_request.url,
                response_status_code=response.status_code,
                response_body=response.content.decode("utf-8"),
                response_header=f'{response.headers}',
            ),
        )

        return json_data


class OzonSellerConnector(OzonConnector):
    """Seller API connector."""

    def __init__(self, *args, **kwargs):
        """Initialize class."""
        super().__init__(*args, **kwargs)
        self.base_url = SELLER_API_URL

    def build_request(
        self,
        params: Dict[str, Any],
        url_params: Optional[Dict[str, Any]] = None,
        query_params: Union[Dict[str, Any], None] = None,
        sandbox: bool = True,
    ) -> Request:
        """Build requests.Request object from config.

        Set params as json dict

        :param params: Dict with json parameters for request
        :type params: Dict[str, Any]
        :param str url_params: Url params
        :type url_params: Dict[str, Any]
        :param bool sandbox: Option for debugging

        :return: requests.Request object prepared for request
        :rtype: requests.Request
        """
        if url_params:
            request_url = self.base_url + self.url.format(**url_params)
        if sandbox:
            request_url = SANDBOX_URL + self.url
        else:
            request_url = self.base_url + self.url

        request = Request(
            method=self.request_type,
            url=request_url,
            params=query_params,
            json=params,
        )

        return request

    def inject_headers(
        self,
        domain: str,
        prepared_request: PreparedRequest,
        sandbox: bool = True,
    ) -> PreparedRequest:
        """Inject seller API required headers."""
        if sandbox:
            host = SANDBOX_URL.replace('http://', '')
            client_id = SANDBOX_SETTINGS['client_id']
            api_key = SANDBOX_SETTINGS['api_key']
        else:
            host = self.base_url.replace('https://', '')
            ozon_auth = OzonAuthKey.objects.get(domain=domain)
            client_id = ozon_auth.client_id
            api_key = ozon_auth.api_key

        prepared_request.headers['Host'] = host
        prepared_request.headers['Client-Id'] = client_id
        prepared_request.headers['Api-Key'] = api_key
        prepared_request.headers['Content-Type'] = 'application/json'

        return prepared_request
