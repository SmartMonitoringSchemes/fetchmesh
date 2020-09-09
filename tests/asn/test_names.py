from pathlib import Path

import pytest

from fetchmesh.asn import ASNames


def test_from_file():
    f = Path(__file__).parent / "data" / "asn.txt"
    names = ASNames.from_file(f)
    assert names.mapping[288] == "ESA European Space Agency (ESA), EU"


@pytest.mark.online
def test_from_url():
    names = ASNames.from_url()
    assert names.mapping[288] == "ESA European Space Agency (ESA), EU"
