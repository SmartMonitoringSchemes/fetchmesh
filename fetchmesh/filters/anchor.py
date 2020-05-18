import random
from dataclasses import dataclass
from itertools import chain
from typing import List, Union

from ..atlas import AtlasAnchor, AtlasAnchorPair
from ..utils import groupby_pairs, sample_groups
from .abstract import BatchFilter

# TODO: AS sampler


class AnchorFilter(BatchFilter[AtlasAnchor]):
    pass


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


class AnchorPairFilter(BatchFilter[AtlasAnchorPair]):
    pass


@dataclass(frozen=True)
class PairSampler(AnchorPairFilter):
    k: Union[float, int]

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


class HalfPairFilter(AnchorPairFilter):
    def filter(self, data):
        kept = set()
        for (a, b) in sorted(data):
            if (b, a) not in kept:
                kept.add((a, b))
        return list(kept)


class SelfPairFilter(AnchorPairFilter):
    # reverse = False: drop self pairs
    # reverse = True: keep only self pairs
    def __init__(self, reverse=False):
        self.reverse = reverse

    def filter(self, data):
        if self.reverse:
            return [x for x in data if x[0] == x[1]]
        return [x for x in data if x[0] != x[1]]
