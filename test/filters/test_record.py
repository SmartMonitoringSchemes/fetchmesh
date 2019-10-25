from fetchmesh.filters import ProbeIDRecordFilter, SelfRecordFilter


def test_probe_record_filter():
    f = ProbeIDRecordFilter({1001})
    r1 = {"prb_id": 1001}
    r2 = {"prb_id": 1002}
    assert f([r1, r2]) == [r1]


def test_self_record_filter():
    f = SelfRecordFilter()
    r1 = {"from": "127.0.0.1", "src_addr": "10.0.0.1", "dst_addr": "8.8.8.8"}
    r2 = {"from": "127.0.0.1", "src_addr": "10.0.0.1", "dst_addr": "10.0.0.1"}
    r3 = {"from": "127.0.0.1", "src_addr": "10.0.0.1", "dst_addr": "127.0.0.1"}
    assert f([r1, r2, r3]) == [r1]
