from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import lru_cache
from ipaddress import ip_address


class RecordTransformer(ABC):
    @abstractmethod
    def transform(self, record: dict) -> dict:
        pass

    def __call__(self, record: dict) -> dict:
        return self.transform(record)


class PingMinimumTransformer(RecordTransformer):
    def transform(self, record: dict) -> dict:
        return {"timestamp": record["timestamp"], "min": record["min"]}


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

    @staticmethod
    @lru_cache(maxsize=65536)
    def is_private(addr: str) -> bool:
        return ip_address(addr).is_private

    def transform(self, record: dict) -> dict:
        hops = []
        for hop in record.get("result", []):
            addrs = []  # type: ignore
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
            if self.as_set:
                addrs = set(addrs)  # type: ignore
            hops.append(addrs)

        return {
            "timestamp": record["timestamp"],
            "from": record["from"],
            "src_addr": record["src_addr"],
            "dst_addr": record["dst_addr"],
            "paris_id": record["paris_id"],
            "hops": hops,
        }
