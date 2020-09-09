import sys
from pathlib import Path

import pytest

from fetchmesh.stubs import requests_get

sys.path.append(str(Path(__file__).parent / "helpers"))


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_get)
