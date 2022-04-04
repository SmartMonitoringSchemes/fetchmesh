import pytest
from click.testing import CliRunner

from fetchmesh.mocks import requests_mock


class Runner(CliRunner):
    def invoke(self, *args, **kwargs):
        result = super().invoke(*args, **kwargs)
        assert result.exit_code == 0


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    cache_get = lambda self, key, fn, *args, **kwargs: fn()
    monkeypatch.setattr("mbox.cache.Cache.get", cache_get)
    monkeypatch.setattr("requests.sessions.Session.request", requests_mock())


@pytest.fixture
def runner():
    cli = Runner()
    with cli.isolated_filesystem():
        yield cli


@pytest.fixture
def tmpfile(tmp_path):
    return tmp_path / "tmpfile"
