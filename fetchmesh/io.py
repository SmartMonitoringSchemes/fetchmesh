import struct
from dataclasses import dataclass, field
from io import TextIOWrapper
from pathlib import Path
from traceback import print_exception
from typing import Iterable, List

from zstandard import ZstdCompressor, ZstdDecompressor

from .filters import StreamFilter
from .utils import json_trydumps, json_tryloads


def detect_codec(file):
    with open(file, "rb") as f:
        try:
            h = struct.unpack("<I", f.read(4))[0]
        except struct.error:
            h = None
    if h == 0xFD2FB528:
        return "zstd"
    return ""


@dataclass
class AtlasRecordsWriter:
    file: Path
    filters: List[StreamFilter[dict]] = field(default_factory=list)
    compression: bool = False
    append: bool = False

    def __enter__(self):
        mode = "ab" if self.append else "wb"
        self.f = self.file.open(mode)
        if self.compression:
            ctx = ZstdCompressor()
            self.f = ctx.stream_writer(self.f)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.flush()
        self.f.close()
        if exc_type:
            self.file.unlink()
            print_exception(exc_type, exc_value, traceback)
        return True

    def write(self, record: dict):
        for filter_ in self.filters:
            if not filter_.keep(record):
                return
        record_ = json_trydumps(record) + "\n"
        self.f.write(record_.encode("utf-8"))

    def writeall(self, records: Iterable[dict]):
        for record in records:
            self.write(record)


@dataclass
class AtlasRecordsReader:
    file: Path
    filters: List[StreamFilter[dict]] = field(default_factory=list)

    def __enter__(self):
        codec = detect_codec(self.file)
        self.f = self.file.open("rb")
        if codec == "zstd":
            ctx = ZstdDecompressor()
            self.f = ctx.stream_reader(self.f)
        self.f = TextIOWrapper(self.f, "utf-8")
        return filter(
            lambda record: all(f.keep(record) for f in self.filters),
            map(json_tryloads, self.f),
        )

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()
        if exc_type:
            print_exception(exc_type, exc_value, traceback)

    # TODO: Handle list, glob pattern, ...
    @classmethod
    def glob(cls, todo):
        pass
