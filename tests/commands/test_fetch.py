from fetchmesh.commands import main


def test_fetch(runner):
    args = """
    fetch --af 4 --type ping --start-date 2020-09-08 --stop-date 2020-09-09
          --sample-pairs 10
    """
    runner.invoke(main, args)


def test_fetch_pairs(runner):
    args = """
    fetch --af 4 --type ping --start-date 2020-09-08 --stop-date 2020-09-09
          --sample-pairs 0.001 --save-pairs
    """
    runner.invoke(main, args)

    args = """
    fetch --af 4 --type ping --start-date 2020-09-08 --stop-date 2020-09-09
          --load-pairs ping_v4_1599523200_1599609600.pairs
    """
    runner.invoke(main, args)
