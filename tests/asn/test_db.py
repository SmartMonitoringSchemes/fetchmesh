from pathlib import Path

from fetchmesh.asn import ASNDB


def test_asndb():
    f1 = Path(__file__).parent.joinpath("rib.txt")
    db1 = ASNDB.from_file(f1)

    f2 = Path(__file__).parent.joinpath("rib.txt.zst")
    db2 = ASNDB.from_file(f2)

    tree1 = db1.radix_tree()
    origins = tree1.search_best("1.23.220.42").data["origins"]

    assert db1 == db2
    assert origins == [45528]
