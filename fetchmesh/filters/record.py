from dataclasses import dataclass
from typing import Set

from .abstract import StreamFilter


@dataclass(frozen=True)
class ProbeIDRecordFilter(StreamFilter[dict]):
    probe_ids: Set[int]

    def keep(self, data):
        return data["prb_id"] in self.probe_ids


class SelfRecordFilter(StreamFilter[dict]):
    def keep(self, data):
        # TODO: data.get(..., None) for old results ?
        return (data["from"] != data["dst_addr"]) and (
            data["src_addr"] != data["dst_addr"]
        )
