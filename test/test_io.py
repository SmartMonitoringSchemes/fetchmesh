from pathlib import Path

from fetchmesh.io import AtlasRecordsReader, AtlasRecordsWriter


class BlackholeFilter:
    def keep(self, record):
        return False


def test_basic():
    file = Path("testfile")
    records = [{"test": "test"}]

    with AtlasRecordsWriter(file) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(file) as r:
        records_ = list(r)

    assert records_ == records


def test_empty():
    file = Path("testfile")
    records = []

    with AtlasRecordsWriter(file) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(file) as r:
        records_ = list(r)

    assert records_ == records


def test_invalid():
    file = Path("testfile")
    records = [None, {"test": "test"}]

    with AtlasRecordsWriter(file) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(file) as r:
        records_ = list(r)

    assert records_ == records


def test_compression():
    file = Path("testfile")
    records = [{"test": "test"}]

    with AtlasRecordsWriter(file, compression=True) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(file) as r:
        records_ = list(r)

    assert records_ == records


def test_filters():
    file = Path("testfile")
    filters = [BlackholeFilter()]
    records = [{"test": "test"}]

    with AtlasRecordsWriter(file, filters=filters, compression=True) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(file) as r:
        records_ = list(r)

    assert records_ == []


def test_exception():
    file = Path("testfile")
    filters = [BlackholeFilter()]
    records = [{"test": "test"}]
    with AtlasRecordsWriter(file, filters=filters, compression=True) as w:
        for record in records:
            w.write(record)
        raise ValueError()
    assert not file.exists()
