import logging
from typing import List, Tuple

from .atlas import AtlasClient
from .io import AtlasRecordsWriter
from .meta import AtlasResultsMeta


class Fetcher:
    def __init__(self, outdir, client=AtlasClient(), filters=[]):
        self.outdir = outdir
        self.client = client
        self.filters = filters
        self.logger = logging.getLogger(__name__)


class SingleFileFetcher(Fetcher):
    def fetch(self, args: Tuple[AtlasResultsMeta, List[int]]):
        # TODO: `Job` NamedTuple instead?
        meta, probes = args
        file = self.outdir.joinpath(meta.filename)
        if file.exists():
            self.logger.debug("%s already exists, skipping...", file)
            return
        it = self.client.fetch_results_stream(meta.remote_path(probes))
        with AtlasRecordsWriter(file, self.filters, meta.compressed) as w:
            w.writeall(it)
