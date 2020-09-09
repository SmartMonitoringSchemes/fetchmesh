from dataclasses import dataclass
from datetime import datetime


@dataclass
class RISCollector:
    name: str

    @property
    def base_url(self) -> str:
        return f"http://data.ris.ripe.net/{self.name}"

    def table_url(self, t: datetime) -> str:
        return "{}/{}/bview.{}.gz".format(
            self.base_url, t.strftime("%Y.%m"), t.strftime("%Y%m%d.%H%M")
        )


@dataclass
class RouteViewsCollector:
    name: str

    @property
    def base_url(self) -> str:
        if self.name == "route-views2":
            return "http://archive.routeviews.org/bgpdata"
        return f"http://archive.routeviews.org/{self.name}/bgpdata"

    def table_url(self, t: datetime) -> str:
        return "{}/{}/RIBS/rib.{}.bz2".format(
            self.base_url, t.strftime("%Y.%m"), t.strftime("%Y%m%d.%H%M")
        )
