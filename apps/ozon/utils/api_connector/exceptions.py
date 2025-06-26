"""Module with exceptions for Ozon urls and connectors."""


class ImportProductRequestSizeLimitExceeded(Exception):
    """You are trying to import over 1000 products."""


class RequestsPerSecondLimitExceeded(Exception):
    """Raises when requests per second limit exceeded."""
