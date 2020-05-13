import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import ceil
from urllib.parse import urlencode

import requests
from tqdm import tqdm

from .cache import Cache

ATLAS_API_URL = "https://atlas.ripe.net/api/v2"


class BaseAtlasClient:
    """
    `requests` wrapper for the Atlas API.
    Handles caching, and concurrent requests for paginated results.
    """

    def __init__(
        self,
        base_url=ATLAS_API_URL,
        page_size=500,
        progress=True,
        threads=4,
        timeout=15,
    ):
        self.base_url = base_url
        self.page_size = page_size
        self.progress = progress
        self.threads = threads
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        self.cache = Cache()

    def encode_url(self, url, params):
        return f"{url}?{urlencode(params)}"

    def get(self, endpoint, params, **kwargs):
        url = f"{self.base_url}/{self.encode_url(endpoint, params)}"
        f = lambda: requests.get(url, timeout=self.timeout, **kwargs)
        return self.cache.get(url, f)

    def get_one(self, endpoint, params={}):
        params = {**params, "page_size": self.page_size}
        obj = self.get(endpoint, params).json()
        return obj["results"], obj["count"]

    def get_all(self, endpoint, params={}):
        results, total = self.get_one(endpoint, params)

        if len(results) < total:
            pages = ceil(total / len(results))
            queue = [(endpoint, {**params, "page": i}) for i in range(2, pages + 1)]
            workers = min(self.threads, len(queue))

            with ThreadPoolExecutor(workers) as executor:
                fn = lambda x: self.get_one(*x)
                futures = as_completed(executor.submit(fn, x) for x in queue)
                if self.progress:
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


class AtlasClient(BaseAtlasClient):
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

    def fetch_results_stream(self, path):
        url = self.base_url + path
        r = requests.get(url, stream=True, timeout=self.timeout)
        if r.status_code != 200:
            self.logger.warning("%s status for GET %s", r.status_code, url)
            return []
        return map(json.loads, filter(None, r.iter_lines(decode_unicode=True)))
