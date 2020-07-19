from dataclasses import dataclass
from typing import Mapping
from urllib.request import urlopen

from mtoolbox.cache import Cache

DEFAULT_NAMES_URL = "ftp://ftp.ripe.net/ripe/asnames/asn.txt"


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
    def from_url(cls, url=DEFAULT_NAMES_URL):
        cache = Cache("fetchmesh")

        def fn():
            with urlopen(url) as f:
                return f.read().decode("ISO8859-1")

        content = cache.get(url, fn)
        return cls.from_str(content)
