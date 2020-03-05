import datetime as dt
import re
from dataclasses import dataclass
from typing import List, Optional
from urllib.parse import urlencode

from cached_property import cached_property

from .atlas import MeasurementAF, MeasurementType
from .utils import parsetimestamp, unwrap


@dataclass(frozen=True)
class AtlasResultsMeta:
    af: MeasurementAF
    type: MeasurementType
    msm_id: int

    start_date: dt.datetime
    stop_date: dt.datetime

    anchors_only: bool
    compressed: bool
    format: str

    # Optional source probes filter
    # TODO: explicit "all", and "unknown" if not provided
    # (e.g. using from_filename())
    probes: List[int]

    EXTENSIONS = {"json": "json", "txt": "ndjson"}

    FORMATS = {v: k for k, v in EXTENSIONS.items()}

    PATTERN = re.compile(r"(\w+)_v(\d)_(-?\d+)_(-?\d+)_(-?\d+)_(\w+)\.([\.\w]+)")

    # TODO: Mark filename, remote_url, ... as properties

    def filename(self, prb_id: Optional[int] = None) -> str:
        content_str = "full"
        if prb_id:
            # TODO: Implement in from_filename
            content_str = str(prb_id)
        elif self.anchors_only:
            content_str = "anchors"

        extension = self.EXTENSIONS[self.format]
        if self.compressed:
            extension += ".zst"

        return "{}_v{}_{}_{}_{}_{}.{}".format(
            self.type.value,
            self.af.value,
            self.start_timestamp(),
            self.stop_timestamp(),
            self.msm_id,
            content_str,
            extension,
        )

    @cached_property
    def remote_path(self) -> str:
        path = f"/measurements/{self.msm_id}/results"
        params = {
            "format": self.format,
            "start": self.start_timestamp(),
            "stop": self.stop_timestamp(),
        }
        # Not supported by the API if set to False
        if self.anchors_only:
            params["anchors-only"] = True
        if self.probes:
            params["probe_ids"] = ",".join(str(x) for x in self.probes)
        return f"{path}?{urlencode(params)}"

    def start_timestamp(self) -> int:
        return int(self.start_date.timestamp())

    def stop_timestamp(self) -> int:
        return int(self.stop_date.timestamp())

    @classmethod
    def from_filename(cls, name: str, probes: List[int] = []):
        m = unwrap(cls.PATTERN.search(name))

        anchors_only = m.group(6) == "anchors"
        extension = m.group(7)
        compressed = False

        if extension[-4:] == ".zst":
            compressed = True
            extension = extension[:-4]

        format_ = cls.FORMATS[extension]

        return cls(
            MeasurementAF(int(m.group(2))),
            MeasurementType(m.group(1)),
            int(m.group(5)),
            unwrap(parsetimestamp(m.group(3))),
            unwrap(parsetimestamp(m.group(4))),
            anchors_only,
            compressed,
            format_,
            probes,
        )
