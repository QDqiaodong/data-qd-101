"""Bounded local HTTP transport; no external publisher connector is implied."""
from urllib.parse import urlsplit, urlencode
from urllib.request import build_opener, HTTPRedirectHandler
from .protocol import MAX_PAGE_BYTES


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise ValueError("redirect is outside the source contract")


def _loopback_address(endpoint):
    address = urlsplit(endpoint)
    if (address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost", "::1"}
            or address.query or address.fragment or address.username or address.password):
        raise ValueError("endpoint must be an unauthenticated loopback HTTP URL without query")
    return endpoint, build_opener(NoRedirect())


def page_fetcher(endpoint):
    endpoint, opener = _loopback_address(endpoint)
    def get(continuation):
        query = {"verb": "ListRecords", "resumptionToken": continuation} if continuation else {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
        with opener.open(endpoint + "?" + urlencode(query), timeout=5) as response:
            return response.read(MAX_PAGE_BYTES + 1)
    return get


def window_fetcher(endpoint):
    """Fetcher for an incremental date window.

    ``first(from_date, until_date)`` sends verb/metadataPrefix plus the frozen
    inclusive ``from``/``until`` dates. ``next_(token)`` sends verb plus the
    fully opaque resumption token only -- no date or metadataPrefix may appear
    on continuation requests. Tokens are passed through verbatim, including
    leading/trailing whitespace and special characters; urlencode preserves
    them inside the query value.
    """
    endpoint, opener = _loopback_address(endpoint)

    def open_query(query):
        with opener.open(endpoint + "?" + urlencode(query), timeout=5) as response:
            return response.read(MAX_PAGE_BYTES + 1)

    class WindowFetcher:
        def first(self, from_date, until_date):
            return open_query({"verb": "ListRecords", "metadataPrefix": "oai_dc",
                               "from": from_date, "until": until_date})

        def next_(self, token):
            if not isinstance(token, str) or token == "":
                raise ValueError("continuation requires a non-empty opaque token")
            return open_query({"verb": "ListRecords", "resumptionToken": token})

    return WindowFetcher()
