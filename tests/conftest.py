import sys
from pathlib import Path

import pytest

from fetchmesh.mocks import requests_mock


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_mock())
