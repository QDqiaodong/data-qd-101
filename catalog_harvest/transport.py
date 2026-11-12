"""Bounded local HTTP transport; no external publisher connector is implied."""
from urllib.parse import urlsplit, urlencode
from urllib.request import build_opener, HTTPRedirectHandler
from .protocol import MAX_PAGE_BYTES


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args):
        raise ValueError("redirect is outside the source contract")


def page_fetcher(endpoint):
    address = urlsplit(endpoint)
    if (address.scheme != "http" or address.hostname not in {"127.0.0.1", "localhost", "::1"}
            or address.query or address.fragment or address.username or address.password):
        raise ValueError("endpoint must be an unauthenticated loopback HTTP URL without query")
    opener = build_opener(NoRedirect())
    def get(continuation):
        query = {"verb": "ListRecords", "resumptionToken": continuation} if continuation else {"verb": "ListRecords", "metadataPrefix": "oai_dc"}
        with opener.open(endpoint + "?" + urlencode(query), timeout=5) as response:
            return response.read(MAX_PAGE_BYTES + 1)
    return get
