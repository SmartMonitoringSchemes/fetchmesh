# Parser for the pyasn database format produced by
# - https://github.com/hadiasghari/pyasn
# - https://github.com/maxmouchet/goasn
# Handles multiple origin ASes (from goasn)
from dataclasses import dataclass
from typing import Mapping

from radix import Radix


class ASNDB:
    def __init__(self, data):
        self._data = data

    def radix_tree(self):
        rtree = Radix()
        for prefix, origins in self._data:
            rnode = rtree.add(prefix)
            rnode.data["origins"] = origins
        return rtree

    @classmethod
    def from_file(cls, file):
        data = []
        with open(file) as f:
            for line in f:
                if line.startswith(";"):
                    continue
                prefix, origins = line.split("\t")
                origins = [int(x) for x in origins.split(",")]
                data.append((prefix, origins))
        return cls(data)