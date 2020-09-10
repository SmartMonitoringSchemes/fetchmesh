from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from fetchmesh.asn import Collector, RISCollector, RouteViewsCollector


def test_from_fqn():
    assert Collector.from_fqdn("rrc00.ripe.net") == RISCollector("rrc00")
    assert Collector.from_fqdn("route-views2.oregon-ix.net") == RouteViewsCollector(
        "route-views2"
    )
    assert Collector.from_fqdn(
        "route-views.amsix.routeviews.org"
    ) == RouteViewsCollector("route-views.amsix")


def test_ris():
    t = datetime(2019, 8, 1, 8)

    c1 = RISCollector("rrc00")
    assert (
        c1.table_url(t)
        == "http://data.ris.ripe.net/rrc00/2019.08/bview.20190801.0800.gz"
    )


def test_routeviews():
    t = datetime(2019, 8, 1, 8)

    c1 = RouteViewsCollector("route-views.amsix")
    assert (
        c1.table_url(t)
        == "http://archive.routeviews.org/route-views.amsix/bgpdata/2019.08/RIBS/rib.20190801.0800.bz2"
    )

    c2 = RouteViewsCollector("route-views2")
    assert (
        c2.table_url(t)
        == "http://archive.routeviews.org/bgpdata/2019.08/RIBS/rib.20190801.0800.bz2"
    )


def test_download_rib():
    t = datetime(2019, 8, 1, 8)
    c = RouteViewsCollector("route-views.amsix")
    with TemporaryDirectory() as tmpdir:
        c.download_rib(t, tmpdir)
        assert (Path(tmpdir) / c.table_name(t)).exists()
