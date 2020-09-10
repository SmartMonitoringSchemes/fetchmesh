import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj
from typing import Optional, Union

import requests


class Collector(ABC):
    """
    Base class for Remote Route Controllers (RRCs).

    .. doctest::

        >>> from datetime import datetime
        >>> from fetchmesh.asn import Collector
        >>> collector = Collector.from_fqdn("route-views2.routeviews.org")
        >>> collector.table_name(datetime(2020, 1, 1, 8))
        'rib.20200101.0800.bz2'
        >>> collector.table_url(datetime(2020, 1, 1, 8))
        'http://archive.routeviews.org/bgpdata/2020.01/RIBS/rib.20200101.0800.bz2'
    """

    extension = ""

    @abstractmethod
    @property
    def fqdn(self) -> str:
        ...

    @abstractmethod
    def table_name(self, t: datetime) -> str:
        """
        Returns the file name for the RIB at time `t`.
        """
        ...

    @abstractmethod
    def table_url(self, t: datetime) -> str:
        """
        Returns the URL for the RIB at time `t`.
        """
        ...

    def download_rib(self, t: datetime, directory: Union[Path, str]) -> Path:
        """
        Download the Routing Information Base (RIB) at time `t` in `directory`.
        """
        file = Path(directory) / self.table_name(t)
        # TODO: Show progress
        if not file.exists():
            r = requests.get(self.table_url(t), stream=True, timeout=15)
            r.raise_for_status()
            with file.open("wb") as f:
                copyfileobj(r.raw, f)
        return file

    @classmethod
    def from_fqdn(cls, fqdn: str) -> Optional["Collector"]:
        m = re.match(r"^(.+)\.(routeviews|oregon-ix|ripe)\.\w+", fqdn)
        if m:
            name, service = m.groups()
            if service == "ripe":
                return RISCollector(name)
            elif service == "routeviews" or service == "oregon-ix":
                return RouteViewsCollector(name)
        return None


@dataclass(frozen=True)
class RISCollector(Collector):
    """
    A Remote Route Collector (RRC) from the `RIPE Routing Information Service`_ (RIS).

    .. code-block:: python

        from fetchmesh.asn import RISCollector
        collector = RISCollector("rrc00")

    .. _RIPE Routing Information Service: https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/ris-raw-data
    """

    name: str
    extension: str = "gz"

    @property
    def base_url(self) -> str:
        return f"http://data.ris.ripe.net/{self.name}"

    @property
    def fqdn(self) -> str:
        return f"{self.name}.ripe.net"

    def table_name(self, t: datetime) -> str:
        return "bview.{}.{}".format(t.strftime("%Y%m%d.%H%M"), self.extension)

    def table_url(self, t: datetime) -> str:
        return "{}/{}/{}".format(self.base_url, t.strftime("%Y.%m"), self.table_name(t))


@dataclass(frozen=True)
class RouteViewsCollector(Collector):
    """
    A Remote Route Collector (RRC) from the `University of Oregon Route Views Project`_.

    .. code-block:: python

        from fetchmesh.asn import RISCollector
        collector = RouteViewsCollector("route-views2")

    .. _University of Oregon Route Views Project: http://archive.routeviews.org/
    """

    name: str
    extension: str = "bz2"

    @property
    def base_url(self) -> str:
        if self.name == "route-views2":
            return "http://archive.routeviews.org/bgpdata"
        return f"http://archive.routeviews.org/{self.name}/bgpdata"

    @property
    def fqdn(self) -> str:
        return f"{self.name}.routeviews.org"

    def table_name(self, t: datetime) -> str:
        return "rib.{}.{}".format(t.strftime("%Y%m%d.%H%M"), self.extension)

    def table_url(self, t: datetime) -> str:
        return "{}/{}/RIBS/{}".format(
            self.base_url, t.strftime("%Y.%m"), self.table_name(t)
        )
