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
    drop_private: bool = False

    @staticmethod
    @lru_cache(maxsize=65536)
    def is_private(addr: str) -> bool:
        return ip_address(addr).is_private

    def transform(self, record: dict) -> dict:
        hops = []
        for hop in record.get("result", []):
            addrs = []
            for reply in hop.get("result", []):
                addr = reply.get("from")
                if self.drop_private and addr and self.is_private(addr):
                    addr = None
                addrs.append(addr)
            hops.append(addrs)

        return {
            "timestamp": record["timestamp"],
            "from": record["from"],
            "src_addr": record["src_addr"],
            "dst_addr": record["dst_addr"],
            "paris_id": record["paris_id"],
            "hops": hops,
        }
