from dataclasses import dataclass


@dataclass(frozen=True)
class IX:
    id: int
    name: str

    @classmethod
    def from_dict(cls, d: dict):
        return cls(d["id"], d["name"])


@dataclass(frozen=True)
class LAN:
    id: int
    ix_id: int

    @classmethod
    def from_dict(cls, d: dict):
        return cls(d["id"], d["ix_id"])


@dataclass(frozen=True)
class Prefix:
    id: int
    ixlan_id: int
    prefix: str

    @classmethod
    def from_dict(cls, d: dict):
        return cls(d["id"], d["ixlan_id"], d["prefix"])
