# pylint: disable=E1101,E1133
from collections import defaultdict

import click
import tableformatter as tf

from ..atlas import MeasurementAF, MeasurementType
from ..ext import DateParseParamType, bprint
from ..filters import HalfPairFilter, MeasurementDateFilter, SelfPairFilter
from ..mesh import AnchoringMesh
from ..utils import countby


def expected_pairs(n_anchors, no_self, half):
    npairs = n_anchors * (n_anchors - 1)
    if half:
        npairs /= 2
    if not no_self:
        npairs += n_anchors
    return int(npairs)


@click.command()
@click.option(
    "--date",
    default="now",
    show_default=True,
    type=DateParseParamType(),
    help="Pairs should exist between `start-date` and `stop-date`.",
)
def describe(date):
    """
    Anchoring mesh overview.
    """

    mesh = AnchoringMesh.from_api().filter(MeasurementDateFilter.running(date, date))

    # TODO: Number of distinct pairs counted, vs theoretical number
    # TODO: Table per country, per AS (tops), plot distribution ?
    bprint("Number of active anchors", len(mesh.anchors))
    bprint("Number of running measurements", len(mesh.measurements))

    pairs_obs = len(mesh.pairs)
    pairs_exp = expected_pairs(len(mesh.anchors), False, False)
    bprint("Number of origin-destination pairs (all)", f"{pairs_obs}/{pairs_exp}")

    pairs_obs = len(mesh.pairs.filter(SelfPairFilter()))
    pairs_exp = expected_pairs(len(mesh.anchors), True, False)
    bprint("Number of origin-destination pairs (noself)", f"{pairs_obs}/{pairs_exp}")

    pairs_obs = len(mesh.pairs.filter(SelfPairFilter()).filter(HalfPairFilter()))
    pairs_exp = expected_pairs(len(mesh.anchors), True, True)
    bprint(
        "Number of origin-destination pairs (noself,half)", f"{pairs_obs}/{pairs_exp}"
    )

    # Intra-region count
    counts = countby(mesh.anchors, lambda x: x.country.main_region)
    rows = [
        ["Anchors"] + list(counts.values()),
        ["Pairs"] + [expected_pairs(count, False, False) for count in counts.values()],
    ]
    cols = [""] + list(counts.keys())
    print(tf.generate_table(rows, cols))

    # Inter-region count
    counts = countby(mesh.anchors, lambda x: x.country.main_region)
    rows = [[k] + [a * b for b in counts.values()] for k, a in counts.items()]
    cols = [""] + list(counts.keys())
    print(tf.generate_table(rows, cols))

    # Top AS counts
    counts = countby(mesh.anchors, lambda x: x.as_v4)
    counts = dict(sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5])
    rows = [
        ["Anchors"] + list(counts.values()),
        ["Pairs"] + [expected_pairs(count, False, False) for count in counts.values()],
    ]
    cols = [""] + list(counts.keys())
    print(tf.generate_table(rows, cols))

    # Types counts
    counts = defaultdict(lambda: defaultdict(int))
    for m in mesh.measurements:
        counts[m.af][m.type] += 1

    rows = [
        [af.name] + [counts[af][t] for t in MeasurementType] for af in MeasurementAF
    ]
    cols = [""] + [x.name for x in MeasurementType]
    print(tf.generate_table(rows, cols))
