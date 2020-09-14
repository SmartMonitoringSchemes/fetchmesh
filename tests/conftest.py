import sys

import pytest

from fetchmesh.mocks import requests_mock


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_mock())


@pytest.fixture
def tmpfile(tmp_path_factory):
    return tmp_path_factory.mktemp("tmp") / "tmpfile"
