import datetime as dt
import re
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional, Tuple
from urllib.parse import urlencode

from cached_property import cached_property

from .countries import countries
from .utils import parsetimestamp, unwrap

ATLAS_API_URL = "https://atlas.ripe.net/api/v2"

ATLAS_RESULTS_META_PATTERN = re.compile(
    r"(\w+)_v(\d)_(-?\d+)_(-?\d+)_(-?\d+)_(\w+)\.([\.\w]+)"
)


class MeasurementAF(Enum):
    IPv4 = 4
    IPv6 = 6


class MeasurementStatus(Enum):
    Specified = 0
    Scheduled = 1
    Ongoing = 2
    # It seems that there is no `3`
    Stopped = 4
    ForcedToStop = 5
    NoSuitableProbes = 6
    Failed = 7
    Archived = 8


class MeasurementType(Enum):
    Ping = "ping"
    Traceroute = "traceroute"
    DNS = "dns"
    SSL = "sslcert"
    HTTP = "http"
    NTP = "ntp"
    WiFi = "wifi"


class Country:
    def __init__(self, country_code):
        try:
            self.info = countries[country_code]
        except:
            self.info = countries["FR"]
            print(country_code)

    def __eq__(self, o):
        return self.country_code == o.country_code

    def __hash__(self):
        return hash(self.country_code)

    @property
    def country_code(self):
        return self.info["code"]

    @property
    def main_region(self):
        return self.info["continent"]


@dataclass(frozen=True)
class AtlasAnchor:
    id: int
    probe_id: int
    fqdn: str
    country: Country
    as_v4: Optional[int]
    as_v6: Optional[int]

    def __lt__(self, o):
        return self.id < o.id

    @classmethod
    def from_dict(cls, d: dict):
        return cls(
            d["id"],
            d["probe"],
            d["fqdn"],
            Country(d["country"]),
            d["as_v4"],
            d["as_v6"],
        )

    def to_dict(self):
        return {
            **self.__dict__,
            "country": self.country.country_code,
            "probe": self.probe_id,
        }


AtlasAnchorPair = Tuple[AtlasAnchor, AtlasAnchor]


@dataclass(frozen=True)
class AtlasMeasurement:
    id: int
    af: MeasurementAF
    type: MeasurementType
    status: MeasurementStatus
    start_date: Optional[dt.datetime]
    stop_date: Optional[dt.datetime]
    description: str
    tags: Tuple[str, ...]

    @cached_property
    def anchor_name(self) -> Optional[str]:
        if self.is_anchoring:
            return self.tags[1]
        return None

    @cached_property
    def anchor_probe(self) -> Optional[int]:
        if self.is_anchoring:
            return int(self.tags[2])
        return None

    @cached_property
    def is_anchoring(self) -> bool:
        return self.is_anchoring_mesh or self.is_anchoring_probes

    @cached_property
    def is_anchoring_mesh(self) -> bool:
        return self.description.startswith("Anchoring Mesh Measurement")

    @cached_property
    def is_anchoring_probes(self) -> bool:
        return self.description.startswith("Anchoring Probes Measurement")

    @classmethod
    def from_dict(cls, d: dict):
        start_date = parsetimestamp(d["start_time"])
        stop_date = None

        status = MeasurementStatus(d["status"]["id"])

        if status not in {
            MeasurementStatus.Specified,
            MeasurementStatus.Scheduled,
            MeasurementStatus.Ongoing,
        }:
            stop_date = parsetimestamp(d["status"]["when"])

        return cls(
            d["id"],
            MeasurementAF(d["af"]),
            MeasurementType(d["type"]),
            status,
            start_date,
            stop_date,
            d["description"],
            tuple(d["tags"]),
        )


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

    def remote_url(self) -> str:
        url = ATLAS_API_URL + f"/measurements/{self.msm_id}/results"
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
        return f"{url}?{urlencode(params)}"

    def start_timestamp(self) -> int:
        return int(self.start_date.timestamp())

    def stop_timestamp(self) -> int:
        return int(self.stop_date.timestamp())

    @classmethod
    def from_filename(cls, name: str, probes: List[int] = []):
        m = unwrap(ATLAS_RESULTS_META_PATTERN.search(name))

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
