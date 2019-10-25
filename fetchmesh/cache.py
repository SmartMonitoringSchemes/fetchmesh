import datetime as dt
import pickle as pkl
from pathlib import Path
from shutil import rmtree

from appdirs import user_cache_dir

from .utils import parsetimestamp

# TODO: Compression (lz4?)


class Cache:
    def __init__(self, name="fetchmesh"):
        self.dir = Path(user_cache_dir(name))
        self.dir.mkdir(parents=True, exist_ok=True)

    def flush(self):
        rmtree(self.dir)
        self.dir.mkdir(parents=True)

    def get(self, key, fn, maxage=dt.timedelta(days=1), invalidate=False):
        file = self.dir.joinpath(key).with_suffix(".pkl")

        if file.exists():
            mtime = parsetimestamp(file.stat().st_mtime)
            expired = dt.datetime.now() - mtime > maxage
            if invalidate or expired:
                file.unlink()

        if not file.exists():
            value = fn()
            with file.open("wb") as f:
                pkl.dump(value, f)

        with file.open("rb") as f:
            return pkl.load(f)
