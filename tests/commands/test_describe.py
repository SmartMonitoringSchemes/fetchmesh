from fetchmesh.commands import main


def test_describe(runner):
    runner.invoke(main, ["describe"])
