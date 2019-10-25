import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import sha256
from math import ceil
from typing import Iterable, List, Tuple
from urllib.parse import urlencode

import requests
from tqdm import tqdm

from .cache import Cache
from .meta import AtlasResultsMeta
from .utils import getLogger

ATLAS_API_URL = "https://atlas.ripe.net/api/v2"


def encode_url(url: str, params: dict) -> str:
    return f"{url}?{urlencode(params)}"


class AtlasClient:
    def __init__(
        self,
        base_url=ATLAS_API_URL,
        cache=Cache(),
        max_workers=4,
        show_progress=True,
        timeout=15,
    ):
        self.base_url = base_url
        self.cache = cache
        self.max_workers = max_workers
        self.show_progress = show_progress
        self.timeout = timeout
        self.logger = getLogger(self)

    def _cache_key(self, s):
        s = s.encode("utf-8")
        h = sha256(s).digest()
        return h.hex()

    def get(self, url, **kwargs):
        f = lambda: requests.get(url, timeout=self.timeout, **kwargs)
        if self.cache:
            k = self._cache_key(url)
            return self.cache.get(k, f)
        return f()

    def get_one(self, endpoint: str, params: dict = {}) -> Tuple[List[dict], int]:
        url = f"{self.base_url}/{encode_url(endpoint, params)}"
        obj = self.get(url).json()
        return obj["results"], obj["count"]

    def get_all(self, endpoint: str, params: dict = {}) -> List[dict]:
        results, total = self.get_one(endpoint, params)

        if len(results) < total:
            npages = ceil(total / len(results))

            queue = []
            for page in range(2, npages + 1):
                params_ = {**params, "page": page}
                queue.append((endpoint, params_))

            workers = min(self.max_workers, len(queue))
            with ThreadPoolExecutor(workers) as executor:
                futures = [
                    executor.submit(lambda x: self.get_one(*x), x) for x in queue
                ]
                futures = tqdm(
                    as_completed(futures),
                    endpoint,
                    disable=not self.show_progress,
                    total=len(queue),
                    leave=False,
                )

                for future in futures:
                    res, _ = future.result()
                    results.extend(res)

        distinct = {x["id"] for x in results}
        if len(distinct) != len(results):
            self.logger.warning(
                "Unexpected number of results: %s vs %s", len(distinct), len(results)
            )

        return results

    def fetch_anchors(self):
        return self.get_all("anchors")

    def fetch_anchoring_measurements(self):
        params = {"include": "measurement,target"}
        return self.get_all("anchor-measurements", params)

    def fetch_results_stream(self, meta: AtlasResultsMeta) -> Iterable[dict]:
        url = meta.remote_url()
        r = requests.get(url, stream=True, timeout=self.timeout)
        if r.status_code != 200:
            self.logger.warn("%s status for GET %s", r.status_code, url)
            return []
        return map(json.loads, filter(None, r.iter_lines(decode_unicode=True)))
