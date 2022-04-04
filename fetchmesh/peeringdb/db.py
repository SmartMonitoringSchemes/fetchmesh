from dataclasses import dataclass
from typing import List

from mbox.itertools import groupby
from radix import Radix

from .client import PeeringDBClient
from .objects import IX, LAN, Prefix


# TODO: Better structure?
# (Embed names, remove ids from prefixes ...)
@dataclass(frozen=True)
class Object:
    ix: IX
    prefixes: List[Prefix]


class PeeringDB:
    """
    An object-oriented interface to `PeeringDB <https://www.peeringdb.com>`_.

    .. code-block:: python

        from fetchmesh.peeringdb import PeeringDB
        peeringdb = PeeringDB.from_api()

        peeringdb.objects[0]
        # Object(ix=IX(id=1, name='Equinix Ashburn'), prefixes=[
        #    Prefix(id=2, ixlan_id=1, prefix='2001:504:0:2::/64'),
        #    Prefix(id=386, ixlan_id=1, prefix='206.126.236.0/22')
        # ])

        ixtree = peeringdb.radix_tree()
        ixtree.search_best("37.49.236.1").data["ix"]
        # IX(id=359, name='France-IX Paris')
    """

    def __init__(self, objects: List[Object]):
        self.objects = objects

    def radix_tree(self) -> Radix:
        """Return a radix tree (from the py-radix library) for fast IP-IX lookups."""
        rtree = Radix()
        for obj in self.objects:
            for prefix in obj.prefixes:
                rnode = rtree.add(prefix.prefix)
                rnode.data["ix"] = obj.ix
        return rtree

    @classmethod
    def from_api(cls, client=PeeringDBClient()) -> "PeeringDB":
        """Load PeeringDB from the PeeringDB API."""
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
