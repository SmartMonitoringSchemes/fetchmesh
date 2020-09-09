import json
from pathlib import Path


class MockResponse:
    def __init__(self, filename):
        self.file = Path(__file__).parent / filename

    @property
    def content(self):
        return self.file.read_text()

    def json(self):
        return json.loads(self.file.read_text())

    def iter_lines(self, *args, **kwargs):
        with self.file.open() as f:
            for line in f:
                yield line

    @property
    def status_code(self):
        return 200


def requests_get(*args, **kwargs):
    if kwargs["url"].endswith("autnums.html"):
        return MockResponse("autnums.html")
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/anchors"):
        return MockResponse("anchors.json")
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/measurements"):
        if "/results" in kwargs["url"]:
            return MockResponse("results.ndjson")
        return MockResponse("measurements.json")
    raise Exception(f"Trying to mock unknown URL, args={args}, kwargs={kwargs}")
