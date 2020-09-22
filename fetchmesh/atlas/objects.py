import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Tuple

from cached_property import cached_property
from mtoolbox.countries import countries, iso2iso3
from mtoolbox.datetime import parsetimestamp
from pytz import UTC


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


@dataclass(frozen=True)
class Country:
    iso2: str

    @property
    def iso3(self):
        return iso2iso3[self.iso2]

    @property
    def main_region(self):
        return countries[self.iso3]["#region+main+name+preferred"]

    @property
    def sub_region(self):
        return countries[self.iso3]["#region+name+preferred+sub"]

    @classmethod
    def from_iso(cls, iso: str) -> "Country":
        if len(iso) == 2:
            return cls(iso.upper())
        if len(iso) == 3:
            return cls(iso2iso3[iso.upper()])
        raise Exception(f"Invalid ISO code: {iso}")


@dataclass(frozen=True)
class AtlasAnchor:
    """A RIPE Atlas anchor."""

    id: int
    """Anchor ID."""

    probe_id: int
    """Anchor **probe** ID."""

    fqdn: str
    """Anchor fully-qualified domain name."""

    country: Country
    """Anchor country."""

    as_v4: Optional[int]
    """Anchor IPv4 autonomous system number (if applicable)."""

    as_v6: Optional[int]
    """Anchor IPv6 autonomous system number (if applicable)."""

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
            Country.from_iso(d["country"]),
            d["as_v4"],
            d["as_v6"],
        )

    def to_dict(self):
        return {
            **self.__dict__,
            "country": self.country.iso2,
            "probe": self.probe_id,
        }


AtlasAnchorPair = Tuple[AtlasAnchor, AtlasAnchor]


@dataclass(frozen=True)
class AtlasMeasurement:
    """A RIPE Atlas measurement."""

    id: int
    """Measurement ID."""

    af: MeasurementAF
    """Measurement IP address family."""

    type: MeasurementType
    """Measurement type."""

    status: MeasurementStatus
    """Measurement status."""

    start_date: Optional[datetime]
    """Measurement start date (if started)."""

    stop_date: Optional[datetime]
    """Measurement stop date (if stopped)."""

    description: str
    """Measurement description."""

    tags: Tuple[str, ...]
    """Measurement tags."""

    ANCHOR_NAME_PATTERN = re.compile(r"^(\w+)-(\w+)-(as\d+).+$")
    ANCHOR_NAME_PATTERN_FALLBACK = re.compile(r"^.+anchor\s+(.+?)\..")
    ANCHOR_PROBE_PATTERN = re.compile(r"^\d+$")

    @cached_property
    def anchor_name(self) -> Optional[str]:
        """Target anchor name, extracted from the tags or from the description."""
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
        """Target anchor probe ID, extracted from the tags."""
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
        """
        Whether the measurement is part of the `anchoring mesh`
        or the `anchoring probes` measurements or not.
        """
        return self.is_anchoring_mesh or self.is_anchoring_probes

    @cached_property
    def is_anchoring_mesh(self) -> bool:
        """Whether the measurement is part of the `anchoring mesh` measurements."""
        return self.description.startswith("Anchoring Mesh Measurement")

    @cached_property
    def is_anchoring_probes(self) -> bool:
        """Whether the measurement is part of the `anchoring probes` measurements."""
        return self.description.startswith("Anchoring Probes Measurement")

    @classmethod
    def from_dict(cls, d: dict):
        """Build from a dict following the Atlas API format."""
        start_date = parsetimestamp(d["start_time"], UTC)
        stop_date = None

        status = MeasurementStatus(d["status"]["id"])

        if status not in {
            MeasurementStatus.Specified,
            MeasurementStatus.Scheduled,
            MeasurementStatus.Ongoing,
        }:
            stop_date = parsetimestamp(d["status"]["when"], UTC)

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
        """Convert to a dict following the Atlas API format."""
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
