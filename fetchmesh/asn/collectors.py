from dataclasses import dataclass
from datetime import datetime


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
