import re
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

    # We rely on the fqdn to compare the anchors, and not on the id.
    # This is more robust if an anchor is decomissionned and replaced by another.
    # For example https://atlas.ripe.net/probes/6118/
    # > This anchor has been decommissioned (removed) as of 2019-09-02 09:01 UTC.
    # > It has been replaced by https://atlas.ripe.net/probes/6593/

    def __eq__(self, o):
        return self.fqdn == o.fqdn

    def __lt__(self, o):
        return self.fqdn < o.fqdn

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

    ANCHOR_NAME_PATTERN = re.compile(r"^(\w+)-(\w+)-(as\d+).+$")
    ANCHOR_NAME_PATTERN_FALLBACK = re.compile(r"^.+anchor\s+(.+?)\..")
    ANCHOR_PROBE_PATTERN = re.compile(r"^\d+$")

    @cached_property
    def anchor_name(self) -> Optional[str]:
        if self.is_anchoring:
            # Unfortunately, tags are not always ordered the same:
            # 1404703:  (anchoring, de-str-as553, 6035, mesh)
            # 23589837: (6683, de-str-as553, mesh, anchoring)
            # return self.tags[1]
            for tag in self.tags:
                if self.ANCHOR_NAME_PATTERN.match(tag):
                    return tag
            # Fallback on measurement description
            match = self.ANCHOR_NAME_PATTERN_FALLBACK.match(self.description)
            if match:
                return match.group(1)
        return None

    @cached_property
    def anchor_probe(self) -> Optional[int]:
        if self.is_anchoring:
            # See comment for `anchor_name`
            # return int(self.tags[2])
            for tag in self.tags:
                if self.ANCHOR_PROBE_PATTERN.match(tag):
                    return int(tag)
            # print(f"Measurement {self.id} (is_anchoring=True): anchor probe not found in tags ({self.tags})")
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

    def to_dict(self):
        start_time = None
        if self.start_date:
            start_time = int(self.start_date.timestamp())
        when = None
        if self.stop_date:
            when = int(self.stop_date.timestamp())
        return {
            "id": self.id,
            "af": self.af.value,
            "type": self.type.value,
            "tags": self.tags,
            "description": self.description,
            "start_time": start_time,
            "status": {"id": self.status.value, "when": when},
        }
