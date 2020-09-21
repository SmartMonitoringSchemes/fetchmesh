import random
from dataclasses import dataclass
from itertools import chain
from typing import List, Union

from mtoolbox.itertools import groupby_pairs
from mtoolbox.random import sample_groups

from ..atlas import AtlasAnchor, AtlasAnchorPair
from .abstract import BatchFilter, StreamFilter


class AnchorFilter(StreamFilter[AtlasAnchor]):
    ...


class AnchorPairFilter(BatchFilter[AtlasAnchorPair]):
    ...


@dataclass(frozen=True)
class AnchorRegionFilter(AnchorFilter):
    region: str

    def keep(self, x):
        return x.country.main_region == self.region


# TODO: Fix this.
# @dataclass(frozen=True)
# class AnchorRegionSampler(AnchorFilter):
#     k: Union[float, int]
#     regions: List[str]
#
#     def population(self, anchors):
#         anchors_grouped = groupby(anchors, lambda x: x.country.iso3_country_code)
#
#         anchors_by_region = []
#         for region in self.regions:
#             countries = Country.get_countries_in_region(region)
#             anchors_in_region: List[AtlasAnchor] = []
#             for country in countries:
#                 anchors_in_region.extend(anchors_grouped.get(country, []))
#             anchors_by_region.append(anchors_in_region)
#
#         return anchors_by_region
#
#     def filter(self, data):
#         groups = sample_groups(self.population(data), self.k)
#         return list(chain.from_iterable(groups))


@dataclass(frozen=True)
class PairSampler(AnchorPairFilter):
    """
    Take a random sample of the anchor pairs.

    .. code-block:: python

        # Keep 75% of the pairs
        filter = PairSampler(0.75)

        # Keep 200 pairs
        filter = PairSampler(200)
    """

    k: Union[float, int]
    """
    | If ``k`` is a float between 0.0 and 1.0, it will sample ``k*len(data)`` pairs.
    | If ``k`` is an integer greater than or equal to 0, it will sample ``k`` pairs.
    """

    def filter(self, data):
        if isinstance(self.k, float) and (self.k < 0.0 or self.k > 1.0):
            raise ValueError("Ratio must be between 0.0 and 1.0")
        if isinstance(self.k, float):
            size = int(len(data) * self.k)
        else:
            size = min(len(data), self.k)
        return random.sample(data, size)


@dataclass(frozen=True)
class PairRegionSampler(AnchorPairFilter):
    k: Union[float, int]
    regions: List[str]

    def population(self, pairs):
        pairs_by_region = groupby_pairs(pairs, lambda x: x.country.main_region)
        population = []
        for region in self.regions:
            if region in pairs_by_region:
                population.append(pairs_by_region[region])
        return population

    def filter(self, data):
        groups = sample_groups(self.population(data), self.k)
        return list(chain.from_iterable(groups))


@dataclass(frozen=True)
class HalfPairFilter(AnchorPairFilter):
    """
    Keep only one of the two measurement for each pair
    (i.e. A->B or B->A, but not both).

    .. code-block:: python

        filter = HalfPairFilter()
    """

    def filter(self, data):
        kept = set()
        for (a, b) in sorted(data):
            if (b, a) not in kept:
                kept.add((a, b))
        return list(kept)


@dataclass(frozen=True)
class SelfPairFilter(AnchorPairFilter):
    """
    Drop (or keep only) measurements where the origin anchor is equal to the destination anchor.

    .. code-block:: python

        # Drop self pairs
        filter = SelfPairFilter()

        # Keep only self pairs
        filter = SelfPairFilter(reverse=True)
    """

    reverse: bool = False
    """
    | reverse = False: drop self pairs
    | reverse = True: keep only self pairs
    """

    def filter(self, data):
        if self.reverse:
            return [x for x in data if x[0] == x[1]]
        return [x for x in data if x[0] != x[1]]
