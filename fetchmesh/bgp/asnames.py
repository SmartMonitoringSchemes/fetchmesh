from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from mbox.cache import Cache
from mbox.requests import FTPAdapter
from requests import Session

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
    def from_file(cls, file):
        content = Path(file).read_text()
        return cls.from_str(content)

    @classmethod
    def from_url(cls, url=DEFAULT_NAMES_URL):
        session = Session()
        session.mount("ftp://", FTPAdapter())

        def fn():
            res = session.get(url, timeout=15)
            res.encoding = "ISO8859-1"
            res.raise_for_status()
            return res.text

        cache = Cache("fetchmesh")
        content = cache.get(url, fn)

        return cls.from_str(content)
