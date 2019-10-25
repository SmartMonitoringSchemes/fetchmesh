from abc import ABC, abstractmethod


class RecordTransformer(ABC):
    @abstractmethod
    def transform(self, record: dict) -> dict:
        pass

    def __call__(self, record: dict) -> dict:
        return self.transform(record)


class PingMinimumTransformer(RecordTransformer):
    def transform(self, record: dict) -> dict:
        return {"timestamp": record["timestamp"], "min": record["min"]}


class TracerouteFlatIPTransformer(RecordTransformer):
    def transform(self, record: dict) -> dict:
        hops = []
        for hop in record.get("result", []):
            addrs = []
            for reply in hop.get("result", []):
                addrs.append(reply.get("from"))
            hops.append(addrs)

        return {
            "timestamp": record["timestamp"],
            "src_addr": record["src_addr"],
            "dst_addr": record["dst_addr"],
            "hops": hops,
        }
