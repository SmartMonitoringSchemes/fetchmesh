import datetime as dt
import re
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Union
from urllib.parse import urlencode

from mtoolbox.datetime import parsetimestamp
from mtoolbox.optional import unwrap
from pytz import UTC

from .asn import Collector
from .atlas import AtlasMeasurement, MeasurementAF, MeasurementType


def meta_from_filename(name: Union[Path, str]):
    for cls in [AtlasResultsMeta, IPASNMeta, RIBMeta]:
        with suppress(Exception):
            return cls.from_filename(name)  # type: ignore


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
    def from_filename(cls, name: Union[Path, str]) -> "AtlasResultsMeta":
        m = unwrap(cls.PATTERN.search(str(name)))
        type, af, start_timestamp, stop_timestamp, msm_id, extension = m.groups()
        return cls(
            MeasurementAF(int(af)),
            MeasurementType(type),
            int(msm_id),
            unwrap(parsetimestamp(start_timestamp, UTC)),
            unwrap(parsetimestamp(stop_timestamp, UTC)),
            extension.endswith(".zst"),
        )

    @classmethod
    def from_measurement(cls, msm: AtlasMeasurement, *args, **kwargs):
        return cls(msm.af, msm.type, msm.id, *args, **kwargs)

    def __str__(self):
        return "<{} {} #{} {} → {}{}>".format(
            self.af.name,
            self.type.name,
            self.msm_id,
            self.start_date.isoformat(),
            self.stop_date.isoformat(),
            " (compressed)" if self.compressed else "",
        )


@dataclass(frozen=True)
class IPASNMeta:
    collector: Collector
    datetime: dt.datetime

    PATTERN = re.compile(r"ipasn_(.+)_(\d{12})")

    @property
    def filename(self) -> str:
        return "ipasn_{}_{}.txt".format(
            self.collector.fqdn, self.datetime.strftime("%Y%m%d%H%M")
        )

    @classmethod
    def from_filename(cls, name: str) -> "IPASNMeta":
        m = unwrap(cls.PATTERN.search(str(name)))
        fqdn, datetime = m.groups()
        return cls(
            unwrap(Collector.from_fqdn(fqdn)),
            dt.datetime.strptime(datetime, "%Y%m%d%H%M").replace(tzinfo=UTC),
        )

    def __str__(self):
        return "<IPASN {} {}>".format(self.collector.fqdn, self.datetime.isoformat())


@dataclass(frozen=True)
class RIBMeta:
    collector: Collector

    # http://archive.routeviews.org/:
    # > MRT RIB and UPDATE files have internal timestamps in the standard Unix format, however the file names are constructed based on the time zone setting of the collector.
    # > The collectors had their time zones set to Pacific Time prior to Feb 3, 2003 at approximately 19:00 UTC. At that time all but one of the existing collectors had their time zones reset to UTC.
    # >The one exception was routeviews.eqix which was not reset to UTC until Feb 1, 2006 at approximately 21:00 UTC.
    # TODO: Use the proper timezone for each collector.
    datetime: dt.datetime

    PATTERN = re.compile(r"rib_(.+)_(\d{12})")

    @property
    def filename(self) -> str:
        return "rib_{}_{}.{}".format(
            self.collector.fqdn,
            self.datetime.strftime("%Y%m%d%H%M"),
            self.collector.extension,
        )

    @classmethod
    def from_filename(cls, name: str) -> "RIBMeta":
        m = unwrap(cls.PATTERN.search(str(name)))
        fqdn, datetime = m.groups()
        return cls(
            unwrap(Collector.from_fqdn(fqdn)),
            dt.datetime.strptime(datetime, "%Y%m%d%H%M").replace(tzinfo=UTC),
        )

    def __str__(self):
        return "<RIBMeta {} {}>".format(self.collector.fqdn, self.datetime.isoformat())
