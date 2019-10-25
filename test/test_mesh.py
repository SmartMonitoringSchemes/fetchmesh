from fetchmesh.mesh import AnchoringMesh, AnchoringMeshPairs
from fetchmesh.meta import MeasurementAF, MeasurementType


def test_pairs_indexing():
    data = [(1, 1), (1, 2)]
    pairs = AnchoringMeshPairs(data)
    assert pairs[1:2] == AnchoringMeshPairs(data[1:2])
    assert pairs[1] == data[1]


def test_pairs_json():
    mesh = AnchoringMesh.from_api()
    pairs = mesh.pairs[:100]
    pairs.to_json("pairs.json")
    pairsp = pairs.from_json("pairs.json")
    assert pairsp == pairs


# TODO: Check that only one measurement
def test_find_measurement():
    pass
