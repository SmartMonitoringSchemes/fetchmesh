import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from shutil import copyfileobj

import requests


def collector_from_name(name):
    m = re.match(r"^(.+)\.(routeviews|oregon-ix|ripe)\.\w+", name)
    name, service = m.groups()
    if service == "ripe":
        return RISCollector(name)
    elif service == "routeviews" or service == "oregon-ix":
        return RouteViewsCollector(name)


def download_rib(c, t, directory):
    file = Path(directory) / c.table_name(t)
    # TODO: Show progress
    if not file.exists():
        r = requests.get(c.table_url(t), stream=True, timeout=15)
        r.raise_for_status()
        with file.open("wb") as f:
            copyfileobj(r.raw, f)
    return file


@dataclass
class RISCollector:
    name: str

    @property
    def base_url(self) -> str:
        return f"http://data.ris.ripe.net/{self.name}"

    def table_name(self, t: datetime) -> str:
        return "bview.{}.gz".format(t.strftime("%Y%m%d.%H%M"))

    def table_url(self, t: datetime) -> str:
        return "{}/{}/{}".format(self.base_url, t.strftime("%Y.%m"), self.table_name(t))


@dataclass
class RouteViewsCollector:
    name: str

    @property
    def base_url(self) -> str:
        if self.name == "route-views2":
            return "http://archive.routeviews.org/bgpdata"
        return f"http://archive.routeviews.org/{self.name}/bgpdata"

    def table_name(self, t: datetime) -> str:
        return "rib.{}.bz2".format(t.strftime("%Y%m%d.%H%M"))

    def table_url(self, t: datetime) -> str:
        return "{}/{}/RIBS/{}".format(
            self.base_url, t.strftime("%Y.%m"), self.table_name(t)
        )
