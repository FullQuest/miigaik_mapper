"""Exceptions module for Ozon API features."""

from requests.exceptions import RequestException
from apps.utils.mongo_utils import MongoReverseException


class OzonProcessingException(Exception):
    """Raises when something calls mutually exclusive actions."""


class OzonParseException(Exception):
    """Raise it if script got invalid API info.

    Example of usage:
        try:
            ...
        except KeyError as err:
            raise OzonParseException from err
    """


# NOTE: if any exception imported to this file
# NOTE: it MUST be used or be inside __all__
__all__ = (
    'RequestException',
    'MongoReverseException',
    'OzonParseException',
    'OzonProcessingException',
)
