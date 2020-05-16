from dataclasses import dataclass
from typing import List

from ..utils import groupby
from .client import PeeringDBClient
from .objects import IX, LAN, Prefix


@dataclass(frozen=True)
class Object:
    ix: IX
    pfxs: List[Prefix]


class PeeringDB:
    def __init__(self, objects: List[Object]):
        self.objects = objects

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

        return objects
