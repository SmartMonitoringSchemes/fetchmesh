import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from shutil import copy
from subprocess import run


def collector_from_name(name):
    m = re.match(r"^(.+)\.(routeviews|oregon-ix|ripe)\.\w+", name)
    name, service = m.groups()
    if service == "ripe":
        return RISCollector(name)
    elif service == "routeviews" or service == "oregon-ix":
        return RouteViewsCollector(name)


# Requires wget (Linux or macOS)!
def download_rib(c, t, directory, stub=False):
    file = Path(directory).joinpath(c.table_name(t))
    if stub:
        stub = Path(__file__).parent.joinpath("stubs", c.stub_name)
        copy(stub, file)
    else:
        args = ["wget", "-N", c.table_url(t)]
        run(args, check=True, cwd=directory)
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

    @property
    def stub_name(self) -> str:
        return "bview.20190417.0800.gz"


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

    @property
    def stub_name(self) -> str:
        return "rib.20180131.0800.bz2"
