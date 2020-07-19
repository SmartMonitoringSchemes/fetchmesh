# pylint: disable=E1101,E1133
from collections import defaultdict

import click
from mtoolbox.click import ParsedDate
from mtoolbox.itertools import countby
from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ..atlas import MeasurementAF, MeasurementType
from ..ext import bprint
from ..filters import HalfPairFilter, MeasurementDateFilter, SelfPairFilter
from ..mesh import AnchoringMesh


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
    type=ParsedDate(settings={"RETURN_AS_TIMEZONE_AWARE": True, "TIMEZONE": "UTC"}),
    help="Pairs should exist between `start-date` and `stop-date`.",
)
def describe(date):
    """
    Anchoring mesh overview.
    """

    console = Console()
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

    print()

    # Intra-region count
    counts = countby(mesh.anchors, lambda x: x.country.main_region)
    counts = sorted(counts.items())

    table = Table(
        title="Intra-region counts", box=box.MINIMAL_HEAVY_HEAD, show_lines=True
    )

    table.add_column("")
    for x in counts:
        table.add_column(x[0])

    table.add_row(Text("Anchors", style="bold"), *[str(x[1]) for x in counts])
    table.add_row(
        Text("Pairs", style="bold"),
        *[str(expected_pairs(x[1], False, False)) for x in counts],
    )

    console.print(table)

    # Inter-region count
    counts = countby(mesh.anchors, lambda x: x.country.main_region)
    counts = sorted(counts.items())

    table = Table(
        title="Inter-region counts", box=box.MINIMAL_HEAVY_HEAD, show_lines=True
    )

    table.add_column("")
    for x in counts:
        table.add_column(x[0])

    for k, a in counts:
        table.add_row(Text(k, style="bold"), *[str(a * b) for _, b in counts])

    console.print(table)

    # Top AS counts
    counts = countby(mesh.anchors, lambda x: x.as_v4)
    counts = dict(sorted(counts.items(), key=lambda x: x[1], reverse=True)[:5])

    table = Table(title="Top AS counts", box=box.MINIMAL_HEAVY_HEAD, show_lines=True)

    table.add_column("")
    for x in counts:
        table.add_column(str(x))

    table.add_row(
        Text("Anchors", style="bold"), *[str(count) for count in counts.values()],
    )
    table.add_row(
        Text("Pairs", style="bold"),
        *[str(expected_pairs(count, False, False)) for count in counts.values()],
    )

    console.print(table)

    # Types counts
    counts = defaultdict(lambda: defaultdict(int))
    for m in mesh.measurements:
        counts[m.af][m.type] += 1

    table = Table(title="Types counts", box=box.MINIMAL_HEAVY_HEAD, show_lines=True)

    table.add_column("")
    for x in MeasurementType:
        table.add_column(x.name)

    for af in MeasurementAF:
        table.add_row(
            Text(af.name, style="bold"), *[str(counts[af][t]) for t in MeasurementType]
        )

    console.print(table)
