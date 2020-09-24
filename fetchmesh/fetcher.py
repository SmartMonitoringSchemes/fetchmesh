import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

from requests.exceptions import ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)

from .atlas import AtlasClient
from .io import AtlasRecordsWriter
from .meta import AtlasResultsMeta

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchJob:
    meta: AtlasResultsMeta
    probes: List[int] = field(default_factory=list)


@dataclass(frozen=True)
class SimpleFetcher:
    """
    Given an AtlasResultsMeta and a list of probes id,
    fetch the results to a single file.
    """

    directory: Path
    client: AtlasClient = field(default_factory=AtlasClient)
    filters: List = field(default_factory=list)
    log: bool = False
    # retry_on_timeout: bool = True

    def __post_init__(self):
        self.directory.mkdir(exist_ok=True, parents=True)

    def fetch(self, job: FetchJob):
        file = self.directory / job.meta.filename
        if file.exists():
            log.debug("%s already exists, skipping...", file)
        else:
            self._fetch(job, file)
        return job

    @retry(
        reraise=True,
        before_sleep=before_sleep_log(log, logging.WARN),
        retry=retry_if_exception_type(ReadTimeout),
        stop=stop_after_attempt(3),
        # NOTE: We don't wait too long before retrying, since
        # the read timeout is already of 15 seconds.
        wait=wait_random(min=1, max=2),
    )
    def _fetch(self, job, file):
        it = self.client.fetch_results_stream(job.meta.remote_path(job.probes))
        # TODO: Is there a risk of duplicate entries if
        # there is a timeout in the middle of a write?
        with AtlasRecordsWriter(
            file, filters=self.filters, compression=job.meta.compressed, log=self.log
        ) as w:
            w.writeall(it)
