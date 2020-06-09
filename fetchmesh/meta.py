import datetime as dt
import re
from dataclasses import dataclass
from urllib.parse import urlencode

from .atlas import MeasurementAF, MeasurementType
from .utils import parsetimestamp, unwrap


@dataclass(frozen=True)
class AtlasResultsMeta:
    """
    Measurement results file metadata.
    A results file contain results from a single measurement,
    with potentially multiple sources.
    """

    af: MeasurementAF
    type: MeasurementType
    msm_id: int

    start_date: dt.datetime
    stop_date: dt.datetime

    compressed: bool

    PATTERN = re.compile(r"(\w+)_v(\d)_(-?\d+)_(-?\d+)_(-?\d+)\.([\.\w]+)")

    def remote_path(self, probes=[]) -> str:
        path = f"/measurements/{self.msm_id}/results"
        params = {
            "anchors-only": True,
            "format": "txt",
            "start": self.start_timestamp,
            "stop": self.stop_timestamp,
        }
        if probes:
            params["probe_ids"] = ",".join(str(x) for x in probes)
        return f"{path}?{urlencode(params)}"

    @property
    def start_timestamp(self) -> int:
        return int(self.start_date.timestamp())

    @property
    def stop_timestamp(self) -> int:
        return int(self.stop_date.timestamp())

    @property
    def filename(self) -> str:
        extension = "ndjson"
        if self.compressed:
            extension += ".zst"
        return "{}_v{}_{}_{}_{}.{}".format(
            self.type.value,
            self.af.value,
            self.start_timestamp,
            self.stop_timestamp,
            self.msm_id,
            extension,
        )

    @classmethod
    def from_filename(cls, name: str):
        m = unwrap(cls.PATTERN.search(name))
        type, af, start_timestamp, stop_timestamp, msm_id, extension = m.groups()
        return cls(
            MeasurementAF(int(af)),
            MeasurementType(type),
            int(msm_id),
            unwrap(parsetimestamp(start_timestamp)),
            unwrap(parsetimestamp(stop_timestamp)),
            extension.endswith(".zst"),
        )
