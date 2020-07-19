from click.testing import CliRunner

from fetchmesh.cli import cli


def test_fetch():
    runner = CliRunner()
    result = runner.invoke(cli, ["fetch", "--af", 4, "--type", "ping", "--dry-run"])
    assert result.exit_code == 0


def test_fetch_pairs():
    runner = CliRunner()

    with runner.isolated_filesystem():
        args = ["fetch", "--af", 4, "--type", "ping"]
        args += ["--sample-pairs", 0.01]
        args += ["--start-date", "2019-1-1", "--stop-date", "2019-1-2"]
        args += ["--dry-run", "--save-pairs"]

        result = runner.invoke(cli, args)
        assert result.exit_code == 0

        args = ["fetch", "--af", 4, "--type", "ping"]
        args += ["--start-date", "2019-1-1", "--stop-date", "2019-1-2"]
        args += [
            "--dry-run",
            "--load-pairs",
            "ping_v4_1546297200_1546383600.pairs",
        ]

        result = runner.invoke(cli, args)
        assert result.exit_code == 0
