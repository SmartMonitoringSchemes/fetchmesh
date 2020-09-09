import json
import os
import sys

import pytest

from fetchmesh.stubs import requests_get

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_get)
