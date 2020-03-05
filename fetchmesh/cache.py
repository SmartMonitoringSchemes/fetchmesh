import pickle
from datetime import datetime, timedelta
from hashlib import sha256
from pathlib import Path

from appdirs import user_cache_dir


class Cache:
    """
    Simple cache that associates to a key the return value of a function.
    On Linux, files are stored in ~/.cache/{name}.
    """

    def __init__(self, name="fetchmesh"):
        self.dir = Path(user_cache_dir(name))
        self.dir.mkdir(parents=True, exist_ok=True)

    def filename(self, key):
        key = key.encode("utf-8")
        return sha256(key).digest().hex() + ".pkl"

    def filepath(self, key):
        name = self.filename(key)
        return self.dir.joinpath(name)

    def flush(self):
        for file in self.dir.glob("*.pkl"):
            file.unlink()

    def get(self, key, fn, maxage=timedelta(days=1), invalidate=False):
        file = self.filepath(key)
        # 1) If the file exists, and it is expired, then delete it.
        if file.exists():
            mtime = datetime.fromtimestamp(file.stat().st_mtime)
            expired = datetime.now() - mtime > maxage
            if invalidate or expired:
                file.unlink()

        # 2) If the file does not exists, run the function and create it.
        if not file.exists():
            value = fn()
            with file.open("wb") as f:
                pickle.dump(value, f)

        # 3) Return the value from the file.
        with file.open("rb") as f:
            return pickle.load(f)
