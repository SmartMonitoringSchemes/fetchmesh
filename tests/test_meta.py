from hypothesis import given
from strategies import atlas_results_metas

from fetchmesh.meta import AtlasResultsMeta


# pylint: disable=E1120
@given(meta=atlas_results_metas())
def test_from_filename(meta):
    assert AtlasResultsMeta.from_filename(meta.filename) == meta
