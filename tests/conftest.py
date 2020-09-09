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
    if kwargs["url"].startswith("https://atlas.ripe.net/api/v2/anchors"):
        return MockResponse(stub_for("anchors.json"))
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/measurements"):
        if "/results" in kwargs["url"]:
            return MockResponse(stub_for("results.ndjson"))
        return MockResponse(stub_for("measurements.json"))
    raise Exception(f"Trying to mock unknown URL, args={args}, kwargs={kwargs}")


def cache_get(self, key, fn, *args, **kwargs):
    # Patch asnames.txt here, since we can't monkeypatch urlopen
    if key == "ftp://ftp.ripe.net/ripe/asnames/asn.txt":
        with open(stub_for("asn.txt")) as f:
            content = f.read()
        return content
    return fn()


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    monkeypatch.setattr("mtoolbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_get)
