import logging

import requests

from ..cache import Cache

PEERINGDB_API_URL = "https://peeringdb.com/api"


class BasePeeringDBClient:
    def __init__(self, base_url=PEERINGDB_API_URL, timeout=15):
        self.base_url = base_url
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.cache = Cache()

    def get(self, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint}"
        f = lambda: requests.get(url, timeout=self.timeout, **kwargs)
        return self.cache.get(url, f).json()["data"]


class PeeringDBClient(BasePeeringDBClient):
    def fetch_ixs(self):
        return self.get("ix.json")

    def fetch_ixlans(self):
        return self.get("ixlan.json")

    def fetch_ixpfxs(self):
        return self.get("ixpfx.json")
