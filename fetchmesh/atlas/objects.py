from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Tuple

from cached_property import cached_property

from .countries import countries


def parsetimestamp(x: Any) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(int(x))
    except TypeError:
        return None


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
        self.info = countries[country_code]

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
    start_date: Optional[datetime]
    stop_date: Optional[datetime]
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
