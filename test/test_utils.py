import json
from datetime import datetime, timedelta

import pytest
from hypothesis import given

from fetchmesh.utils import (
    countby,
    daterange,
    groupby,
    groupby_pairs,
    groupby_stream,
    parsetimestamp,
    sample_groups,
    totimestamp,
    tryfunc,
    unwrap,
)
from strategies import atlas_datetimes


def test_countby():
    elements = [(0, "a"), (0, "a"), (0, "b"), (1, "b")]

    counts = countby(elements, lambda x: x[0])
    assert counts == {0: 3, 1: 1}

    counts = countby(elements, lambda x: x[1])
    assert counts == {"a": 2, "b": 2}


def test_groupby():
    elements = [(0, "a"), (0, "a"), (0, "b"), (1, "b")]

    groups = groupby(elements, lambda x: x[0])
    assert groups == {0: [(0, "a"), (0, "a"), (0, "b")], 1: [(1, "b")]}

    groups = groupby(elements, lambda x: x[1])
    assert groups == {"a": [(0, "a"), (0, "a")], "b": [(0, "b"), (1, "b")]}


def test_groupby_stream():
    elements = [(0, "a"), (0, "a"), (0, "b"), (1, "b")]

    groups = list(groupby_stream(elements, lambda x: x[0], 100))
    assert groups == [(0, [(0, "a"), (0, "a"), (0, "b")]), (1, [(1, "b")])]

    groups = list(groupby_stream(elements, lambda x: x[0], 1))
    assert groups == [
        (0, [(0, "a")]),
        (0, [(0, "a")]),
        (0, [(0, "b")]),
        (1, [(1, "b")]),
    ]


def test_groupby_pairs():
    # TODO: Better test
    pairs = [(0, 0), (0, 1), (1, 0), (1, 1)]
    groups = groupby_pairs(pairs, lambda x: x)
    assert groups == {0: [(0, 0)], 1: [(1, 1)]}


def test_sample_groups():
    population = [[None for _ in range(10)] for _ in range(20)]

    groups = sample_groups(population, 0.2)
    assert all(len(group) == 2 for group in groups)

    groups = sample_groups(population, 8)
    assert all(len(group) == 8 for group in groups)

    with pytest.raises(ValueError):
        sample_groups(population, 2.0)


@given(dt=atlas_datetimes())
def test_timestamp(dt):
    assert parsetimestamp(None) == None
    assert parsetimestamp(totimestamp(dt)) == dt


def test_unwrap():
    with pytest.raises(ValueError):
        unwrap(None)
    assert unwrap(True)


def test_tryfunc():
    f = tryfunc(json.loads)
    assert f("[invalid") == None
    assert f('{"a": 1}') == {"a": 1}

    f = tryfunc(json.loads, default="")
    assert f("[invalid") == ""


def test_daterange():
    start = datetime(2019, 1, 1)
    stop = datetime(2019, 1, 2)

    dates = list(daterange(start, stop, timedelta(days=1)))
    assert dates == [start]

    dates = list(daterange(start, stop, timedelta(hours=12)))
    assert dates == [start, start + timedelta(hours=12)]
