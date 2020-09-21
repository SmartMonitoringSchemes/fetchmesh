from hypothesis import given
from hypothesis.provisional import domains
from strategies import atlas_results_metas, collector_datetimes, collector_fqdns

from fetchmesh.bgp import Collector
from fetchmesh.meta import AtlasResultsMeta, IPASNMeta, RIBMeta, meta_from_filename


# pylint: disable=E1120
@given(meta=atlas_results_metas())
def test_atlas_results_meta(meta):
    assert AtlasResultsMeta.from_filename(meta.filename) == meta
    assert meta_from_filename(meta.filename) == meta


# pylint: disable=E1120
@given(t=collector_datetimes(), fqdn=collector_fqdns())
def test_rib_meta(t, fqdn):
    c = Collector.from_fqdn(fqdn)
    m = RIBMeta(c, t)
    assert RIBMeta.from_filename(m.filename) == m
    assert meta_from_filename(m.filename) == m


# pylint: disable=E1120
@given(t=collector_datetimes(), fqdn=collector_fqdns())
def test_ipasn_meta(t, fqdn):
    c = Collector.from_fqdn(fqdn)
    m = IPASNMeta(c, t)
    assert IPASNMeta.from_filename(m.filename) == m
    assert meta_from_filename(m.filename) == m
