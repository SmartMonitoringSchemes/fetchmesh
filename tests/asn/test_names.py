from pathlib import Path

from fetchmesh.asn import ASNames


def test_asnames():
    f = Path(__file__).parent / "data" / "asn.txt"
    names = ASNames.from_file(f)
    assert names.mapping[288] == "ESA European Space Agency (ESA), EU"
