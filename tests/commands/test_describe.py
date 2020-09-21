import pytest
from click.testing import CliRunner

from fetchmesh.commands import main


def test_describe():
    runner = CliRunner()
    result = runner.invoke(main, ["describe"])
    assert result.exit_code == 0
