import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

import requests
from mtoolbox.cache import Cache

#  DEFAULT_NAMES_URL = "ftp://ftp.ripe.net/ripe/asnames/asn.txt"
DEFAULT_NAMES_URL = "https://www.cidr-report.org/as2.0/autnums.html"


@dataclass(frozen=True)
class ASNames:
    mapping: Mapping[int, str]

    @classmethod
    def from_str(cls, s):
        mapping = {}
        for line in s.splitlines():
            asn, name = line.split(" ", maxsplit=1)
            mapping[int(asn)] = name
        return cls(mapping)

    @classmethod
    def from_file(cls, file):
        content = Path(file).read_text()
        return cls.from_str(content)

    @classmethod
    def from_url(cls, url=DEFAULT_NAMES_URL):
        content = Cache("fetchmesh").get(url, lambda: requests.get(url).content)
        pattern = re.compile(r".+AS(\d+)\s*<\/a>\s*(.+)")
        mapping = {}
        for line in content.split("\n"):
            m = pattern.match(line)
            if m:
                mapping[int(m.group(1))] = m.group(2)
        return cls(mapping)
