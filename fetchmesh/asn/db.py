# Parser for the pyasn database format produced by
# - https://github.com/hadiasghari/pyasn
# - https://github.com/maxmouchet/goasn
# Handles multiple origin ASes (from goasn)
from dataclasses import dataclass
from io import TextIOWrapper
from typing import Mapping

from mtoolbox.magic import CompressionFormat, detect_compression
from radix import Radix
from zstandard import ZstdDecompressor


class ASNDB:
    def __init__(self, data):
        self._data = data

    def __eq__(self, o):
        return self._data == o._data

    def radix_tree(self):
        rtree = Radix()
        for prefix, origins in self._data:
            rnode = rtree.add(prefix)
            rnode.data["origins"] = origins
        return rtree

    @classmethod
    def from_file(cls, file):
        data = []
        # TODO: Unified "file loader"
        # (detect_codec is used is many place)
        codec = detect_compression(file)
        with open(file, "rb") as f:
            if codec == CompressionFormat.Zstandard:
                ctx = ZstdDecompressor()
                f = ctx.stream_reader(f)
            f = TextIOWrapper(f, "utf-8")
            for line in f:
                if line.startswith(";"):
                    continue
                prefix, origins = line.split("\t")
                origins = [int(x) for x in origins.split(",")]
                data.append((prefix, origins))
        return cls(data)
