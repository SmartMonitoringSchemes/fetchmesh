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
                fn = lambda x: self.get_one(*x)
                futures = as_completed(executor.submit(fn, x) for x in queue)
                if self.show_progress:
                    futures = tqdm(futures, endpoint, total=len(queue), leave=False)
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

    def fetch_measurements(self, params):
        return self.get_all("measurements", params)

    # This endpoint is currently broken, it returns random
    # results. Email sent to Atlas support on 2020/02/17.
    # def fetch_anchoring_measurements(self):
    #     params = {"include": "measurement,target"}
    #     return self.get_all("anchor-measurements", params)

    # Alternative way to get the same results
    # as the /anchor-measurements endpoint.
    def fetch_anchoring_measurements(self):
        anchors = self.fetch_anchors()
        measurements = self.fetch_measurements(
            {"description__startswith": "Anchoring Mesh Measurement:"}
        )

        targets = {x["fqdn"]: x for x in anchors}
        results = []
        missing = set()

        for x in measurements:
            if not x["target"] in targets:
                missing.add(x["target"])
                continue
            target = targets[x["target"]]
            results.append({"measurement": x, "target": target})

        if len(missing) > 0:
            self.logger.warning("%s missing anchors", len(missing))

        return results

    def fetch_results_stream(self, meta: AtlasResultsMeta) -> Iterable[dict]:
        url = meta.remote_url()
        r = requests.get(url, stream=True, timeout=self.timeout)
        if r.status_code != 200:
            self.logger.warn("%s status for GET %s", r.status_code, url)
            return []
        return map(json.loads, filter(None, r.iter_lines(decode_unicode=True)))
