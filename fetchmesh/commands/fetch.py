import datetime as dt
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from traceback import print_exc

import click
from tqdm import tqdm

from ..atlas import MeasurementAF, MeasurementType
from ..ext import DateParseParamType, EnumChoice, PathParamType, bprint, format_args
from ..fetcher import SingleFileFetcher
from ..filters import (
    HalfPairFilter,
    MeasurementDateFilter,
    MeasurementTypeFilter,
    PairSampler,
    SelfPairFilter,
)
from ..mesh import AnchoringMesh, AnchoringMeshPairs
from ..meta import AtlasResultsMeta
from ..utils import daterange, totimestamp


def default_dir(
    af: MeasurementAF,
    type_: MeasurementType,
    start_date: dt.datetime,
    stop_date: dt.datetime,
) -> Path:
    start_time = totimestamp(start_date)
    stop_time = totimestamp(stop_date)
    return Path(f"{type_.value}_v{af.value}_{start_time}_{stop_time}")


@click.command()
@click.option(
    "--af",
    required=True,
    show_default=True,
    type=EnumChoice(MeasurementAF, int),
    help="Measurement IP address family",
)
@click.option(
    "--type",
    required=True,
    show_default=True,
    type=EnumChoice(MeasurementType, str),
    help="Measurement type",
)
@click.option(
    "--start-date",
    default="last week",
    show_default=True,
    type=DateParseParamType(),
    help="Results start date",
)
@click.option(
    "--stop-date",
    default="now",
    show_default=True,
    type=DateParseParamType(),
    help="Results stop date",
)
@click.option(
    "--split", metavar="HOURS", type=int, help="Split the results files every X hours"
)
@click.option(
    "--sample-pairs", default=1.0, show_default=True, help="Fraction of pairs to keep"
)
@click.option(
    "--no-self",
    default=False,
    show_default=True,
    is_flag=True,
    help="Remove measurements where the src and dst probe are equal",
)
@click.option(
    "--half",
    default=False,
    show_default=True,
    is_flag=True,
    help="Fetch only one measurement out of two for each pair (A<->B or B<->A), useful for RTT measurements",
)
@click.option(
    "--jobs",
    default=1,
    show_default=True,
    metavar="N",
    type=click.IntRange(min=1),
    help="Number of parallel jobs to run",
)
@click.option(
    "--dir", type=PathParamType(), help="Output directory",
)
@click.option(
    "--dry-run",
    default=False,
    show_default=True,
    is_flag=True,
    help="Don't actually fetch results",
)
@click.option(
    "--compress",
    default=False,
    show_default=True,
    is_flag=True,
    help="Compress results using zstd",
)
@click.option(
    "--save-pairs", type=PathParamType(), help="Save selected pairs to file",
)
@click.option(
    "--load-pairs",
    type=PathParamType(),
    help="Load pairs from file (filters will still be applied!)",
)
def fetch(**args):
    """
    Fetch measurement results.
    """
    bprint("Args", format_args(args))

    start_date = args["start_date"]
    stop_date = args["stop_date"]
    if start_date > stop_date:
        raise click.ClickException("start_date must be before stop_date")

    defdir = default_dir(args["af"], args["type"], start_date, stop_date)
    outdir = args["dir"] or defdir
    bprint("Path", outdir.absolute())

    mesh = AnchoringMesh.from_api()
    bprint("Anchors", len(mesh.anchors))

    # We load pairs either:
    # 1) Directly from a file
    if args["load_pairs"]:
        bprint("Pairs File", args["load_pairs"])
        pairs = AnchoringMeshPairs.from_json(args["load_pairs"])
    # 2) From the anchoring mesh
    else:
        filters = [
            MeasurementDateFilter.running(start_date, stop_date),
            MeasurementTypeFilter(args["af"], args["type"]),
        ]
        for f in filters:
            mesh = mesh.filter(f)
            bprint(f"Anchors > {type(f).__name__}", len(mesh.anchors))
        pairs = mesh.pairs

    bprint("Pairs", len(pairs))

    filters = []
    if args["half"]:
        filters.append(HalfPairFilter())
    if args["no_self"]:
        filters.append(SelfPairFilter())
    if args["sample_pairs"]:
        # This filter must be last
        filters.append(PairSampler(args["sample_pairs"]))

    for f in filters:
        pairs = pairs.filter(f)
        bprint(f"Pairs > {type(f).__name__}", len(pairs))

    if args["save_pairs"]:
        bprint(f"Pairs File", args["save_pairs"])
        pairs.to_json(str(args["save_pairs"]))

    if args["split"]:
        split = dt.timedelta(hours=args["split"])
    else:
        split = stop_date - start_date

    metas = []
    for target, probes in pairs.by_target():
        measurement = mesh.find_measurement(target, args["af"], args["type"])
        if not measurement:
            raise click.ClickException(f"No measurement found for {target}")
        for date in daterange(start_date, stop_date, split):
            meta = AtlasResultsMeta(
                measurement.af,
                measurement.type,
                measurement.id,
                date,
                date + split,
                True,
                args["compress"],
                "txt",
                probes,
            )
            metas.append(meta)

    fetcher = SingleFileFetcher(outdir)

    # Stop here if we perform a dry run
    if args["dry_run"]:
        return

    outdir.mkdir(exist_ok=True, parents=True)

    with ProcessPoolExecutor(args["jobs"]) as executor:
        # TODO: Retry on error
        futures = [executor.submit(fetcher.fetch, meta) for meta in metas]
        futures = tqdm(as_completed(futures), total=len(metas))
        for future in futures:
            try:
                future.result()
            except:
                print_exc()
