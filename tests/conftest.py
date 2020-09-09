import json
import os
import sys

import pytest

sys.path.append(os.path.join(os.path.dirname(__file__), "helpers"))


def stub_for(filename):
    return os.path.join(os.path.dirname(__file__), "stubs", filename)


class MockResponse:
    def __init__(self, file):
        self.file = file

    @property
    def content(self):
        with open(self.file) as f:
            content = f.read()
        return content

    def json(self):
        with open(self.file) as f:
            obj = json.load(f)
        return obj

    def iter_lines(self, *args, **kwargs):
        with open(self.file) as f:
            for line in f:
                yield line

    @property
    def status_code(self):
        return 200


def requests_get(*args, **kwargs):
    if kwargs["url"].endswith("autnums.html"):
        return MockResponse(stub_for("autnums.html"))
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/anchors"):
        return MockResponse(stub_for("anchors.json"))
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/measurements"):
        if "/results" in kwargs["url"]:
            return MockResponse(stub_for("results.ndjson"))
        return MockResponse(stub_for("measurements.json"))
    raise Exception(f"Trying to mock unknown URL, args={args}, kwargs={kwargs}")


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_get)
