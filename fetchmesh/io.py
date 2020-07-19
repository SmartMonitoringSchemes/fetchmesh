import json
import struct
from dataclasses import dataclass, field
from io import TextIOWrapper
from pathlib import Path
from traceback import print_exception
from typing import Iterable, List

from mtoolbox.magic import CompressionFormat, detect_compression
from mtoolbox.optional import tryfunc
from zstandard import ZstdCompressor, ZstdDecompressor

from .filters import StreamFilter
from .transformers import RecordTransformer

json_trydumps = tryfunc(json.dumps, default="")
json_tryloads = tryfunc(json.loads)


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
    transformers: List[RecordTransformer] = field(default_factory=list)

    def __post_init__(self):
        self.file = Path(self.file)

    def __enter__(self):
        codec = detect_compression(self.file)
        self.f = self.file.open("rb")
        if codec == CompressionFormat.Zstandard:
            ctx = ZstdDecompressor()
            self.f = ctx.stream_reader(self.f)
        self.f = TextIOWrapper(self.f, "utf-8")
        stream = filter(
            lambda record: all(f.keep(record) for f in self.filters),
            map(json_tryloads, self.f),
        )
        for f in self.transformers:
            stream = map(f, stream)
        return stream

    def __exit__(self, exc_type, exc_value, traceback):
        self.f.close()
        if exc_type:
            print_exception(exc_type, exc_value, traceback)

    @classmethod
    def all(cls, files, **kwargs):
        for file in files:
            with cls(Path(file), **kwargs) as rdr:
                for record in rdr:
                    yield record

    @classmethod
    def glob(cls, path, pattern, **kwargs):
        files = Path(path).glob(pattern)
        return cls.all(files, **kwargs)
