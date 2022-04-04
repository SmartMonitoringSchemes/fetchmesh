import json
import struct
from dataclasses import dataclass, field
from io import TextIOWrapper
from pathlib import Path
from traceback import print_exception
from typing import Iterable, List, Optional

from mbox.magic import CompressionFormat, detect_compression
from mbox.optional import tryfunc
from zstandard import ZstdCompressionDict, ZstdCompressor, ZstdDecompressor

from .filters import StreamFilter
from .transformers import RecordTransformer

json_trydumps = tryfunc(json.dumps, default="")
json_tryloads = tryfunc(json.loads)

dictionary = Path(__file__).parent / "mocks" / "dictionary"
"""
Path to the zstandard dictionary used to compress the records.
Useful to decompress manually the records.
"""

LogEntry = struct.Struct("LLL")
"""
Binary structure containing the size, the measurement ID, and the probe ID for a record.
This is useful for indexing the content of a result file without decompressing and parsing
the JSON.
If the file is compressed, the size is the size of the zstandard frame.
The fields are unsigned longs of 8 bytes each : `size_bytes`, `msm_id`, `prb_id`.
"""


@dataclass
class AtlasRecordsWriter:
    """
    Write Atlas results in ND-JSON format.

    .. code-block:: python

        from fetchmesh.io import AtlasRecordsWriter
        with AtlasRecordsWriter("results.ndjson") as w:
            w.write({"msm_id": 1001, "prb_id": 1, "...": "..."})
    """

    file: Path
    """Output file path."""

    filters: List[StreamFilter[dict]] = field(default_factory=list)
    """List of filters to apply before writing the records."""

    append: bool = False
    """
    Whether to create a new file, or to append the records to an existing file.
    If append is set to false, and the output file already exists, it will be deleted.
    When append is set to false, the output file will be deleted if an exception happens.
    """

    log: bool = False
    """Record the size (in bytes) of each record. See :any:`LogEntry`."""

    compression: bool = False
    """
    Compresse the records using zstandard.
    We use the one-shot compression API and write one frame per record.
    This results in larger files than a single frame for all the records,
    but it allows us to build an index and make the file seekable.
    We use a pre-built dictionary (see :any:`dictionary`) to reduce the size of the compressed records.
    """

    compression_ctx: Optional[ZstdCompressor] = field(default=None, init=False)

    @property
    def log_file(self) -> Path:
        """Path to the (optional) log file."""
        return self.file.with_suffix(self.file.suffix + ".log")

    def __post_init__(self):
        self.file = Path(self.file)

    def __enter__(self):
        mode = "ab" if self.append else "wb"

        # (1) Open the output file
        self.f = self.file.open(mode)

        # (2) Open the log file
        if self.log:
            self.log_f = self.log_file.open(mode)

        # (3) Setup the compression context
        if self.compression:
            dict_data = ZstdCompressionDict(dictionary.read_bytes())
            self.compression_ctx = ZstdCompressor(dict_data=dict_data)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # (1) Close the output file
        self.f.flush()
        self.f.close()

        # (2) Close the log file
        if self.log:
            self.log_f.flush()
            self.log_f.close()

        # (3) Handle exceptions
        # NOTE: In append mode, we do not delete the file.
        if exc_type:
            if not self.append:
                self.file.unlink()
            if not self.append and self.log_file.exists():
                self.log_file.unlink()
            print_exception(exc_type, exc_value, traceback)

        # Do not reraise exceptions, excepted for KeyboardInterrupt.
        return exc_type is not KeyboardInterrupt

    def write(self, record: dict):
        """Write a single record."""

        # (1) Filter the record
        for filter_ in self.filters:
            if not filter_.keep(record):
                return

        # (2) Serialize and encode the record
        data = json_trydumps(record) + "\n"
        data = data.encode("utf-8")

        # (3) Compresse the record
        if self.compression_ctx:
            data = self.compression_ctx.compress(data)

        # (4) Update the log
        if self.log:
            entry = LogEntry.pack(len(data), record["msm_id"], record["prb_id"])
            self.log_f.write(entry)

        # (5) Write the record to the output file
        self.f.write(data)

    def writeall(self, records: Iterable[dict]):
        """Write all the records."""

        for record in records:
            self.write(record)


@dataclass
class AtlasRecordsReader:
    """
    Read Atlas results in ND-JSON format.
    Automatically handles compressed files.

    .. code-block:: python

        from fetchmesh.io import AtlasRecordsReader

        # From a single file.
        with AtlasRecordsReader("results.ndjson") as r:
            for record in r:
                print(record)

        # From multiple files.
        r = AtlasRecordsReader.all(["results1.ndjson", "results2.ndjson"])
        for record in r:
            print(record)

        # From a glob pattern.
        r = AtlasRecordsReader.glob("mydir/", "*.ndjson")
        for record in r:
            print(record)
    """

    file: Path
    """Input file path."""

    filters: List[StreamFilter[dict]] = field(default_factory=list)
    """List of filters to apply when reading the records."""

    transformers: List[RecordTransformer] = field(default_factory=list)
    """List of transformers to apply when reading the records."""

    def __post_init__(self):
        self.file = Path(self.file)

    def __enter__(self):
        codec = detect_compression(self.file)

        # (1) Open the input file

        # We keep a reference (`f`) to the original file,
        # in order to be able to close it on exit.
        # Calling `close()` on `ZstdDecompressor` does not
        # close the underlying resource.
        self.f = self.file.open("rb")

        # UTF-8 decoded, decompressed, file
        self.fb = self.f

        # (2) Setup the decompressor, if needed
        if codec == CompressionFormat.Zstandard:
            dict_data = ZstdCompressionDict(dictionary.read_bytes())
            ctx = ZstdDecompressor(dict_data=dict_data)
            self.fb = ctx.stream_reader(self.fb, read_across_frames=True)

        # (3) Decode the file
        self.fb = TextIOWrapper(self.fb, "utf-8")

        # (4) Deserialize the records
        stream = map(json_tryloads, self.fb)

        # (5) Apply the filters
        stream = filter(
            lambda record: all(fn.keep(record) for fn in self.filters), stream
        )

        # (6) Apply the transformers
        for fn in self.transformers:
            stream = map(fn, stream)

        return stream

    def __exit__(self, exc_type, exc_value, traceback):
        self.fb.close()
        self.f.close()
        if exc_type:
            print_exception(exc_type, exc_value, traceback)
        # Do not reraise exceptions, excepted for KeyboardInterrupt.
        return exc_type is not KeyboardInterrupt

    @classmethod
    def all(cls, files, **kwargs):
        """Read multiple files."""
        for file in files:
            with cls(Path(file), **kwargs) as rdr:
                for record in rdr:
                    yield record

    @classmethod
    def glob(cls, path, pattern, **kwargs):
        """Read multiple files from a glob pattern."""
        files = Path(path).glob(pattern)
        return cls.all(files, **kwargs)
