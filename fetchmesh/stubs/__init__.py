import json
from pathlib import Path
from shutil import copy


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
    if kwargs["url"] == "https://peeringdb.com/api/ix.json":
        return MockResponse("ix.json")
    elif kwargs["url"] == "https://peeringdb.com/api/ixlan.json":
        return MockResponse("ixlan.json")
    elif kwargs["url"] == "https://peeringdb.com/api/ixpfx.json":
        return MockResponse("ixpfx.json")
    elif kwargs["url"].endswith("autnums.html"):
        return MockResponse("autnums.html")
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/anchors"):
        return MockResponse("anchors.json")
    elif kwargs["url"].startswith("https://atlas.ripe.net/api/v2/measurements"):
        if "/results" in kwargs["url"]:
            return MockResponse("results.ndjson")
        return MockResponse("measurements.json")
    raise Exception(f"Trying to mock unknown URL; args={args}, kwargs={kwargs}")


def wget(url, cwd=None, *args, **kwargs):
    if url.startswith("http://archive.routeviews.org"):
        src = Path(__file__).parent / "rib.20180131.0800.bz2"
        dst = Path(cwd or "") / url.split("/")[-1]
        copy(src, dst)
    elif url.startswith("http://data.ris.ripe.net"):
        src = Path(__file__).parent / "bview.20190417.0800.gz"
        dst = Path(cwd or "") / url.split("/")[-1]
        copy(src, dst)
    else:
        raise Exception(
            f"Trying to mock unknown URL; url={url}, args={args}, kwargs={kwargs}"
        )
