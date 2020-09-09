import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from subprocess import run


def collector_from_name(name):
    m = re.match(r"^(.+)\.(routeviews|oregon-ix|ripe)\.\w+", name)
    name, service = m.groups()
    if service == "ripe":
        return RISCollector(name)
    elif service == "routeviews" or service == "oregon-ix":
        return RouteViewsCollector(name)


# Requires wget (Linux or macOS)!
def download_rib(c, t, directory):
    file = Path(directory) / c.table_name(t)
    wget(c.table_url(t), cwd=directory, options=["-N"])
    return file


def wget(url, cwd=None, options=[]):
    args = ["wget", *options, url]
    run(args, check=True, cwd=cwd)


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
