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
    # Different names in
    # - https://ftp.ripe.net/ripe/asnames/asn.txt
    # - http://www.cidr-report.org/as2.0/autnums.htmlautnums.html...
    # assert names.mapping[288] == "ESA European Space Agency (ESA), EU"
    assert names.mapping[288] == "ESA Robert Bosch Strasse 5, EU"
