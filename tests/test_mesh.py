from datetime import datetime

from pytz import UTC

from fetchmesh.atlas import MeasurementAF, MeasurementType
from fetchmesh.filters import HalfPairFilter, MeasurementDateFilter, SelfPairFilter
from fetchmesh.mesh import AnchoringMesh, AnchoringMeshPairs


def test_pairs_indexing():
    data = [(1, 1), (1, 2)]
    pairs = AnchoringMeshPairs(data)
    assert pairs[1:2] == AnchoringMeshPairs(data[1:2])
    assert pairs[1] == data[1]


def test_pairs_json(tmp_path):
    mesh = AnchoringMesh.from_api()
    pairs = mesh.pairs[:100]
    pairs.to_json(tmp_path / "pairs.json")
    pairsp = pairs.from_json(tmp_path / "pairs.json")
    assert pairsp == pairs


def test_mesh_json(tmp_path):
    mesh = AnchoringMesh.from_api()
    mesh.to_json(tmp_path / "mesh.json")
    meshp = mesh.from_json(tmp_path / "mesh.json")
    assert meshp == mesh


def test_mesh_filter():
    mesh = AnchoringMesh.from_api()
    filters = [
        MeasurementDateFilter.running(
            datetime(2019, 1, 1, tzinfo=UTC), datetime(2020, 1, 1, tzinfo=UTC)
        ),
        MeasurementDateFilter.running(
            datetime(2019, 1, 1, tzinfo=UTC), datetime(2019, 6, 1, tzinfo=UTC)
        ),
    ]
    m1 = mesh.filter(filters)
    m2 = mesh.filter(filters[0]).filter(filters[1])
    assert m1 == m2


def test_pairs_filter():
    # pylint: disable=no-member
    mesh = AnchoringMesh.from_api()
    filters = [SelfPairFilter(), HalfPairFilter()]
    p1 = mesh.pairs.filter(filters)
    p2 = mesh.pairs.filter(SelfPairFilter()).filter(HalfPairFilter())
    assert p1 == p2


# TODO: Check that we find only one measurement
def test_find_measurement():
    pass
