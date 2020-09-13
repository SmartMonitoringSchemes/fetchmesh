import json
from pathlib import Path

from zstandard import ZstdCompressionDict, ZstdDecompressor

from fetchmesh.io import AtlasRecordsReader, AtlasRecordsWriter, LogEntry, dictionary


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


def test_log(tmpfile):
    records = [{"msm_id": 1234, "prb_id": 5678}, {"msm_id": 9876, "prb_id": 5432}]

    with AtlasRecordsWriter(tmpfile, compression=True, log=True) as w:
        log_file = w.log_file
        for record in records:
            w.write(record)

    # TODO: Methods to simplify log reading?
    # Zstandard decompression context
    dict_data = ZstdCompressionDict(dictionary.read_bytes())
    ctx = ZstdDecompressor(dict_data=dict_data)

    f = tmpfile.open("rb")
    log_f = log_file.open("rb")

    log = LogEntry.iter_unpack(log_f.read())
    for i, (size, msm_id, prb_id) in enumerate(log):
        rec = json.loads(ctx.decompress(f.read(size)).decode("utf-8"))
        assert rec == records[i]
        assert msm_id == records[i]["msm_id"]
        assert prb_id == records[i]["prb_id"]

    f.close()
    log_f.close()


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
