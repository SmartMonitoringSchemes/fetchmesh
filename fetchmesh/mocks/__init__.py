from pathlib import Path

from mbox.requests import RequestsMock


def requests_mock():
    root = Path(__file__).parent
    mock = RequestsMock()
    mock.register(r".*ix.json$", root / "ix.json")
    mock.register(r".*ixlan.json$", root / "ixlan.json")
    mock.register(r".*ixpfx.json$", root / "ixpfx.json")
    mock.register(r".*asn.txt$", root / "asn.txt")
    mock.register(r".*/anchors", root / "anchors.json")
    mock.register(r".*/measurements/.+/results", root / "results.ndjson")
    mock.register(r".*/measurements", root / "measurements.json")
    mock.register(r".*archive.routeviews.org.*", root / "rib.20180131.0800.bz2")
    mock.register(r".*data.ris.ripe.net.*", root / "bview.20190417.0800.gz")
    return mock.request
