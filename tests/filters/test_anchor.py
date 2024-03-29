import pytest
from hypothesis import given
from strategies import atlas_results_metas

from fetchmesh.atlas import AtlasAnchor, Country
from fetchmesh.filters import (
    AnchorRegionFilter,
    HalfPairFilter,
    PairRegionSampler,
    PairSampler,
    SelfPairFilter,
)


def test_anchor_region_filter():
    anchors = [
        AtlasAnchor(1, 1, "1", Country("FR"), None, None),
        AtlasAnchor(2, 2, "2", Country("FR"), None, None),
        AtlasAnchor(3, 3, "3", Country("FR"), None, None),
        AtlasAnchor(4, 4, "4", Country("NL"), None, None),
        AtlasAnchor(5, 5, "5", Country("NL"), None, None),
        AtlasAnchor(6, 6, "6", Country("NL"), None, None),
        AtlasAnchor(7, 7, "7", Country("US"), None, None),
    ]
    f = AnchorRegionFilter("Europe")
    assert f(anchors) == anchors[:-1]


def test_half_pair_filter():
    # The filter must produce reproducible results,
    # no matter the initial ordering of the pairs.
    f = HalfPairFilter()
    pairs = [("a", "b"), ("b", "a")]
    assert f(pairs) == f(reversed(pairs))


def test_self_pair_filter():
    pairs = [("a", "a"), ("a", "b")]
    f = SelfPairFilter()
    assert f(pairs) == [("a", "b")]
    f = SelfPairFilter(reverse=True)
    assert f(pairs) == [("a", "a")]


def test_pair_sampler():
    pairs = [("a", "b") for _ in range(10)]
    f = PairSampler(0.5)
    assert f(pairs) == pairs[:5]
    f = PairSampler(1.0)
    assert f(pairs) == pairs
    f = PairSampler(1)
    assert f(pairs) == pairs[:1]
    f = PairSampler(5)
    assert f(pairs) == pairs[:5]
    with pytest.raises(ValueError):
        f = PairSampler(2.0)
        f(pairs)


def test_pair_region_sampler():
    anchors = [
        AtlasAnchor(1, 1, "1", Country("FR"), None, None),
        AtlasAnchor(2, 2, "2", Country("FR"), None, None),
        AtlasAnchor(3, 3, "3", Country("FR"), None, None),
        AtlasAnchor(4, 4, "4", Country("US"), None, None),
        AtlasAnchor(5, 5, "5", Country("NL"), None, None),
        AtlasAnchor(6, 6, "6", Country("NL"), None, None),
        AtlasAnchor(7, 7, "7", Country("NL"), None, None),
    ]
    pairs = [
        (anchors[0], anchors[1]),
        (anchors[0], anchors[1]),
        (anchors[0], anchors[1]),
        (anchors[0], anchors[6]),
        (anchors[3], anchors[3]),
    ]

    st = PairRegionSampler(1000, ["Europe", "Americas"])
    # print(st.sample(pairs, 100))


# def test_anchor_region_sampler():
#     # TODO: Hypothesis for anchors generation
#     anchors = [
#         AtlasAnchor(1, 1, "1", Country("FR"), None, None),
#         AtlasAnchor(2, 2, "2", Country("FR"), None, None),
#         AtlasAnchor(3, 3, "3", Country("FR"), None, None),
#         AtlasAnchor(4, 4, "4", Country("US"), None, None),
#         AtlasAnchor(5, 5, "5", Country("NL"), None, None),
#         AtlasAnchor(6, 6, "6", Country("NL"), None, None),
#         AtlasAnchor(7, 7, "7", Country("NL"), None, None),
#     ]
#
#     st = AnchorRegionSampler(1000, ["Europe", "Northern America"])
#     assert set(st(anchors)) == set(anchors)
