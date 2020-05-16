from dataclasses import dataclass
from typing import List

from radix import Radix

from ..utils import groupby
from .client import PeeringDBClient
from .objects import IX, LAN, Prefix


# TODO: Better structure?
# (Embed names, remove ids from prefixes ...)
@dataclass(frozen=True)
class Object:
    ix: IX
    prefixes: List[Prefix]


class PeeringDB:
    def __init__(self, objects: List[Object]):
        self.objects = objects

    def radix_tree(self):
        rtree = Radix()
        for obj in self.objects:
            for prefix in obj.prefixes:
                rnode = rtree.add(prefix.prefix)
                rnode.data["ix"] = obj.ix
        return rtree

    @classmethod
    def from_api(cls, client=PeeringDBClient()):
        ixs = [IX.from_dict(x) for x in client.fetch_ixs()]
        lans = [LAN.from_dict(x) for x in client.fetch_ixlans()]
        pfxs = [Prefix.from_dict(x) for x in client.fetch_ixpfxs()]

        pfxs_by_lan = groupby(pfxs, lambda x: x.ixlan_id)
        lans_by_ix = groupby(lans, lambda x: x.ix_id)

        objects = []

        for ix in ixs:
            if not ix.id in lans_by_ix:
                continue
            pfxs_ = []
            for lan in lans_by_ix[ix.id]:
                if not lan.id in pfxs_by_lan:
                    continue
                pfxs_.extend(pfxs_by_lan[lan.id])
            objects.append(Object(ix, pfxs_))

        return PeeringDB(objects)
