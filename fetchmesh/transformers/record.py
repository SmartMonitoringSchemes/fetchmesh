from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from ipaddress import ip_address
from typing import Optional, Tuple

from radix import Radix


class RecordTransformer(ABC):
    @abstractmethod
    def transform(self, record: dict) -> dict:
        pass

    def __call__(self, record: dict) -> dict:
        return self.transform(record)


class PingMinimumTransformer(RecordTransformer):
    def transform(self, record: dict) -> dict:
        return {"timestamp": record["timestamp"], "min": record["min"]}


# TODO: More generic TracerouteMapAddrTransformer?
# Takes key and fn(key) -> val as input.


@dataclass
class TracerouteMapASNTransformer(RecordTransformer):
    tree: Radix

    def transform(self, record: dict) -> dict:
        new_record = record.copy()
        for i, hop in enumerate(record.get("result", [])):
            for j, reply in enumerate(hop.get("result", [])):
                try:
                    # TODO: Handle multiple origins
                    # TODO: Cache lookups
                    asn = self.tree.search_best(reply["from"]).data["origins"][0]
                    new_record["result"][i]["result"][j]["asn"] = asn
                except (AttributeError, KeyError):
                    new_record["result"][i]["result"][j]["asn"] = None
        return new_record


@dataclass
class TracerouteMapIXTransformer(RecordTransformer):
    tree: Radix

    def transform(self, record: dict) -> dict:
        new_record = record.copy()
        for i, hop in enumerate(record.get("result", [])):
            for j, reply in enumerate(hop.get("result", [])):
                try:
                    # TODO: Handle multiple origins
                    # TODO: Cache lookups
                    ix = self.tree.search_best(reply["from"]).data["ix"].name
                    new_record["result"][i]["result"][j]["ix"] = ix
                except (AttributeError, KeyError):
                    new_record["result"][i]["result"][j]["ix"] = None
        return new_record


@dataclass
class TracerouteFlatIPTransformer(RecordTransformer):

    as_set: bool = False
    """
    Return each hop as a set instead of a list of addresses.
    """

    drop_dup: bool = False
    """
    Drop duplicate results, e.g.
        {'dup': True, 'from': '203.181.249.93', 'rtt': 280.552, 'size': 28, 'ttl': 231}
         ^^^^^^^^^^^
    """

    drop_late: bool = False
    """
    Drop late results, e.g.
        {"from":"4.68.72.66","late":2,"size":68,"ttl":56}
                              ^^^^^^^^
    """

    drop_private: bool = False
    """
    Drop private IP addresses:
        - 10.0.0.0/8
        - 172.16.0.0/12
        - 192.168.0.0/16
        - fd00::/8
    """

    extras_fields: Tuple[str, ...] = tuple()
    """
    List of additional response fields to include (e.g. `asn`, `ix` ...)
    """

    @staticmethod
    @lru_cache(maxsize=65536)
    def is_private(addr: str) -> bool:
        return ip_address(addr).is_private

    def transform(self, record: dict) -> dict:
        hops = []
        extras = defaultdict(list)  # type: ignore
        for hop in record.get("result", []):
            addrs = []  # type: ignore
            extras_ = defaultdict(list)  # type: ignore
            for reply in hop.get("result", []):
                # Sometimes results contains an empty object {}
                if not reply:
                    continue
                if self.drop_dup and reply.get("dup"):
                    continue
                if self.drop_late and reply.get("late"):
                    continue
                addr = reply.get("from")
                if self.drop_private and addr and self.is_private(addr):
                    addr = None
                addrs.append(addr)
                for field in self.extras_fields:
                    extras_[field].append(reply.get(field))
            if self.as_set:
                addrs = set(addrs)  # type: ignore
                extras_ = {k: set(v) for k, v in extras_.items()}  # type: ignore
            for field in self.extras_fields:
                extras[field].append(extras_.get(field, {}))
            hops.append(addrs)

        return {
            "timestamp": record["timestamp"],
            "from": record["from"],
            "src_addr": record["src_addr"],
            "dst_addr": record["dst_addr"],
            "paris_id": record["paris_id"],
            "hops": hops,
            **extras,
        }
