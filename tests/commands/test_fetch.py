import pytest
from click.testing import CliRunner

from fetchmesh.cli import cli


def test_fetch():
    runner = CliRunner()

    with runner.isolated_filesystem():
        args = ["fetch", "--af", 4, "--type", "ping"]
        args += ["--start-date", "2020-09-08", "--stop-date", "2020-09-09"]
        result = runner.invoke(cli, args)
        assert result.exit_code == 0


def test_fetch_pairs():
    runner = CliRunner()

    with runner.isolated_filesystem():
        args = ["fetch", "--af", 4, "--type", "ping"]
        args += ["--sample-pairs", 0.5]
        args += ["--start-date", "2020-09-08", "--stop-date", "2020-09-09"]
        args += ["--save-pairs"]

        result = runner.invoke(cli, args)
        assert result.exit_code == 0

        args = ["fetch", "--af", 4, "--type", "ping"]
        args += ["--start-date", "2020-09-08", "--stop-date", "2020-09-09"]
        args += ["--load-pairs", "ping_v4_1599523200_1599609600.pairs"]

        result = runner.invoke(cli, args)
        assert result.exit_code == 0
