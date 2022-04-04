import atexit
import datetime as dt
import signal
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from traceback import print_exc

import click
import psutil
from mbox.click import EnumChoice, ParsedDate, PathParam
from mbox.datetime import datetimetuplerange, totimestamp
from tqdm import tqdm

from ..atlas import MeasurementAF, MeasurementType
from ..fetcher import FetchJob, SimpleFetcher
from ..filters import (
    AnchorRegionFilter,
    HalfPairFilter,
    MeasurementDateFilter,
    MeasurementTypeFilter,
    PairSampler,
    SelfPairFilter,
)
from ..mesh import AnchoringMesh, AnchoringMeshPairs
from ..meta import AtlasResultsMeta
from .common import format_args, print_args, print_kv


def cleanup():
    # multiprocessing is buggy and leaves zombies everywhere,
    # so let's do it the hard way.
    parent = psutil.Process()
    children = parent.children(recursive=True)  # + [parent]
    for child in children:
        print("Terminating process {}".format(child.pid))
        child.send_signal(signal.SIGTERM)
    psutil.wait_procs(children)


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
    type=ParsedDate(settings={"RETURN_AS_TIMEZONE_AWARE": True, "TIMEZONE": "UTC"}),
    help="Results start date (UTC)",
)
@click.option(
    "--stop-date",
    default="now",
    show_default=True,
    type=ParsedDate(settings={"RETURN_AS_TIMEZONE_AWARE": True, "TIMEZONE": "UTC"}),
    help="Results stop date (UTC)",
)
@click.option(
    "--region",
    default=None,
    metavar="REGION",
    type=str,
    help="Keep only anchors located in the specified region (e.g. Europe)",
)
@click.option(
    "--split", metavar="HOURS", type=int, help="Split the results files every X hours"
)
@click.option(
    "--sample-pairs",
    default=1.0,
    show_default=True,
    help="If <= 1, fraction of pairs to keep. If > 1, number of pairs to keep.",
)
@click.option(
    "--no-self",
    default=False,
    show_default=True,
    is_flag=True,
    help="Omit the measurements for which the source and destination probes are the same",
)
@click.option(
    "--only-self",
    default=False,
    show_default=True,
    is_flag=True,
    help="Omit the measurements for which the source and destination probes are **not** the same",
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
    "--dir",
    type=PathParam(),
    help="Output directory",
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
    help="Compress the results with the Zstandard algorithm",
)
@click.option(
    "--save-pairs",
    default=False,
    show_default=True,
    is_flag=True,
    help="Save metadata (`$dir.meta`) and pairs (`$dir.pairs`) for reproducibility.",
)
@click.option(
    "--load-pairs",
    type=PathParam(),
    help="Load pairs from file (filters will still be applied!)",
)
def fetch(**args):
    """
    Fetch measurement results from the anchoring mesh.
    """
    print_args(args, fetch)

    start_date = args["start_date"]
    stop_date = args["stop_date"]
    if start_date > stop_date:
        raise click.ClickException("start_date must be before stop_date")

    defdir = default_dir(args["af"], args["type"], start_date, stop_date)
    outdir = args["dir"] or defdir
    print_kv("Path", outdir.absolute())

    mesh = AnchoringMesh.from_api()
    print_kv("Anchors", len(mesh.anchors))

    # We load pairs either:
    # 1) Directly from a file
    if args["load_pairs"]:
        print_kv("Pairs File", args["load_pairs"])
        pairs = AnchoringMeshPairs.from_json(args["load_pairs"])
    # 2) From the anchoring mesh
    else:
        filters = [
            MeasurementDateFilter.running(start_date, stop_date),
            MeasurementTypeFilter(args["af"], args["type"]),
        ]
        if args["region"]:
            filters.append(AnchorRegionFilter(args["region"]))
        for f in filters:
            mesh = mesh.filter(f)
            print_kv(f"Anchors > {type(f).__name__}", len(mesh.anchors))
        pairs = mesh.pairs

    print_kv("Pairs", len(pairs))

    filters = []
    if args["half"]:
        filters.append(HalfPairFilter())
    if args["no_self"]:
        filters.append(SelfPairFilter())
    if args["only_self"]:
        filters.append(SelfPairFilter(reverse=True))
    if args["sample_pairs"]:
        # This filter must be the last one
        sample_pairs = args["sample_pairs"]
        if sample_pairs > 1:
            sample_pairs = int(sample_pairs)
        filters.append(PairSampler(sample_pairs))

    for f in filters:
        pairs = pairs.filter(f)
        print_kv(f"Pairs > {type(f).__name__}", len(pairs))

    if args["save_pairs"]:
        pairs_file = outdir.with_suffix(".pairs")
        meta_file = outdir.with_suffix(".meta")
        print_kv(f"Pairs File", pairs_file)
        pairs.to_json(pairs_file)
        print_kv(f"Meta File", meta_file)
        args_blacklist = {"dry_run", "sample_pairs", "save_pairs"}
        args_fetch = {k: v for k, v in args.items() if k not in args_blacklist}
        args_fetch["load_pairs"] = pairs_file
        meta_str = f"# Run this file with `bash {meta_file}`."
        meta_str += f"\n# Generated on {dt.datetime.now()}."
        meta_str += f"\n# Command that generated this file:"
        meta_str += f"\n# fetchmesh fetch {format_args(args, fetch)}"
        meta_str += f"\n# Command to fetch the results:"
        meta_str += f"\nfetchmesh fetch {format_args(args_fetch, fetch)}"
        meta_file.write_text(meta_str)

    split = stop_date - start_date
    if args["split"]:
        split = dt.timedelta(hours=args["split"])

    jobs = []
    for target, probes in pairs.by_target():
        measurement = mesh.find_measurement(target, args["af"], args["type"])
        for start, stop in datetimetuplerange(start_date, stop_date, split):
            meta = AtlasResultsMeta.from_measurement(
                measurement,
                start_date=start,
                stop_date=stop,
                compressed=args["compress"],
            )
            jobs.append(FetchJob(meta, probes))

    # Stop here if we perform a dry run
    if args["dry_run"]:
        return

    fetcher = SimpleFetcher(outdir)
    atexit.register(cleanup)

    with ProcessPoolExecutor(args["jobs"]) as executor:
        futures = [executor.submit(fetcher.fetch, job) for job in jobs]
        futures = tqdm(as_completed(futures), total=len(jobs))
        for future in futures:
            try:
                future.result()
            except Exception:
                print_exc()

    atexit.unregister(cleanup)
