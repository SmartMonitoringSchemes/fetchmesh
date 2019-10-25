from click.testing import CliRunner

from fetchmesh.cli import cli


def test_describe():
    runner = CliRunner()
    result = runner.invoke(cli, ["describe"])
    assert result.exit_code == 0
