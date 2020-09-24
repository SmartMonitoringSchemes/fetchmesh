from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from tqdm import tqdm

from ..transformers import TracerouteFlatIPTransformer


class KaparFormatter:
    def __init__(self, drop_dup=True, drop_late=True, drop_private=True):
        self.transformer = TracerouteFlatIPTransformer(
            drop_dup=drop_dup,
            drop_late=drop_late,
            drop_private=drop_private,
            insert_none=True,
        )

    def format_record(self, record: dict) -> str:
        record = self.transformer(record)
        # See kapar/lib/PathLoader.cc#436 for the format of the comment line.
        lines = [f"# trace: {record['from']} -> {record['dst_addr']}"]
        for replies in record["hops"]:
            lines.append(replies[0] or "0.0.0.0")
        return "\n".join(lines)

    def format_records(self, records: Iterable[dict], progress: bool = False) -> str:
        lines = []
        for record in tqdm(records, disable=not progress, desc="KaparFormatter"):
            lines.append(self.format_record(record))
        return "\n".join(lines)
