from fetchmesh.cache import Cache


def test_get():
    cache = Cache("fetchmesh_test")
    cache.flush()

    assert cache.get("test", lambda: 1) == 1
    assert cache.get("test", lambda: 2) == 1
    assert cache.get("test", lambda: 2, invalidate=True) == 2

    cache.flush()
    assert cache.get("test", lambda: 3) == 3
