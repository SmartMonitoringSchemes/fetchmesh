from pathlib import Path

from fetchmesh.commands import main


def test_unpack(runner):
    fetch_dir = Path("fetch_dir")
    pairs_dir = Path("pairs_dir")

    # Fetch some measurement results
    args = f"fetch --af 4 --type ping --dir {fetch_dir} --sample-pairs 2"
    runner.invoke(main, args)

    n1 = len(list(fetch_dir.glob("*")))
    assert n1 > 0

    # Invalid file/directories should not crash unpack
    (fetch_dir / "invalid_dir.ndjson").mkdir()
    (fetch_dir / "invalid.ndjson").touch()

    # AF and type do not match fetched results, output dir should be empty
    args = f"unpack --af 6 --type traceroute {fetch_dir} {pairs_dir}"
    runner.invoke(main, args)

    n2 = len(list(pairs_dir.glob("*")))
    assert n2 == 0

    # AF and type match, output dir should not be empty
    args = f"unpack --af 4 --type ping {fetch_dir} {pairs_dir}"
    runner.invoke(main, args)

    n3 = len(list(pairs_dir.glob("*")))
    assert n3 >= n1

    mtime1 = {f: f.stat().st_mtime for f in pairs_dir.glob("*")}
    size1 = {f: f.stat().st_size for f in pairs_dir.glob("*")}

    # In skip mode the files should not be recreated
    args = f"unpack --af 4 --type ping --mode skip {fetch_dir} {pairs_dir}"
    result = runner.invoke(main, args)

    mtime2 = {f: f.stat().st_mtime for f in pairs_dir.glob("*")}
    size2 = {f: f.stat().st_size for f in pairs_dir.glob("*")}

    assert mtime1 == mtime2
    assert size1 == size2

    # In overwrite mode the files should be recreated
    args = f"unpack --af 4 --type ping --mode overwrite {fetch_dir} {pairs_dir}"
    result = runner.invoke(main, args)

    mtime3 = {f: f.stat().st_mtime for f in pairs_dir.glob("*")}
    size3 = {f: f.stat().st_size for f in pairs_dir.glob("*")}

    assert mtime3 != mtime2
    assert size3 == size2

    # In append mode the files should be modified
    args = f"unpack --af 4 --type ping --mode append {fetch_dir} {pairs_dir}"
    result = runner.invoke(main, args)

    mtime4 = {f: f.stat().st_mtime for f in pairs_dir.glob("*")}
    size4 = {f: f.stat().st_size for f in pairs_dir.glob("*")}

    assert mtime4 != mtime3
    assert size4 != size3
