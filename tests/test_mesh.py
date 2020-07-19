from fetchmesh.atlas import MeasurementAF, MeasurementType
from fetchmesh.mesh import AnchoringMesh, AnchoringMeshPairs


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


def test_mesh_json():
    mesh = AnchoringMesh.from_api()
    mesh.to_json("mesh.json")
    meshp = mesh.from_json("mesh.json")
    assert meshp == mesh


# TODO: Check that we find only one measurement
def test_find_measurement():
    pass
