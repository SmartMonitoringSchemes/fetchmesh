from pathlib import Path

from fetchmesh.io import AtlasRecordsReader, AtlasRecordsWriter


class BlackholeFilter:
    def keep(self, record):
        return False


def test_basic(tmpfile):
    records = [{"test": "test"}]

    with AtlasRecordsWriter(tmpfile) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(tmpfile) as r:
        records_ = list(r)

    assert records_ == records


def test_empty(tmpfile):
    records = []

    with AtlasRecordsWriter(tmpfile) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(tmpfile) as r:
        records_ = list(r)

    assert records_ == records


def test_invalid(tmpfile):
    records = [None, {"test": "test"}]

    with AtlasRecordsWriter(tmpfile) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(tmpfile) as r:
        records_ = list(r)

    assert records_ == records


def test_compression(tmpfile):
    records = [{"test": "test"}, {"123": 123}]

    with AtlasRecordsWriter(tmpfile, compression=True) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(tmpfile) as r:
        records_ = list(r)

    assert records_ == records


def test_append(tmpfile):
    records = [{"test": "test"}, {"123": 123}]

    for compression in [False, True]:
        with AtlasRecordsWriter(tmpfile, compression=compression) as w:
            for record in records:
                w.write(record)

        with AtlasRecordsWriter(tmpfile, append=True, compression=compression) as w:
            for record in records:
                w.write(record)

        with AtlasRecordsReader(tmpfile) as r:
            records_ = list(r)

        assert records_ == [*records, *records]


def test_filters(tmpfile):
    filters = [BlackholeFilter()]
    records = [{"test": "test"}]

    with AtlasRecordsWriter(tmpfile, filters=filters, compression=True) as w:
        for record in records:
            w.write(record)

    with AtlasRecordsReader(tmpfile) as r:
        records_ = list(r)

    assert records_ == []


def test_exception(tmpfile):
    filters = [BlackholeFilter()]
    records = [{"test": "test"}]
    with AtlasRecordsWriter(tmpfile, filters=filters, compression=True) as w:
        for record in records:
            w.write(record)
        raise ValueError()
    assert not tmpfile.exists()
