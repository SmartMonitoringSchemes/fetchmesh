from collections import defaultdict
from multiprocessing import Pool

import click
from mtoolbox.click import EnumChoice, ParsedDate, PathParam
from mtoolbox.itertools import groupby_stream
from tqdm import tqdm

from ..atlas import MeasurementAF, MeasurementType
from ..io import AtlasRecordsReader, AtlasRecordsWriter
from ..meta import AtlasResultsMeta
from .common import print_args, print_kv


class UnpackWorker:
    def __init__(self, src, dst, mode):
        self.src = src
        self.dst = dst
        self.mode = mode

    def do(self, metas):
        metas = sorted(metas, key=lambda x: x.start_date)
        start_timestamp = metas[0].start_timestamp
        stop_timestamp = metas[-1].stop_timestamp
        key = lambda x: (x["msm_id"], x["prb_id"])
        seen = set()
        skip = set()
        for meta in metas:
            file = self.src.joinpath(meta.filename)
            with AtlasRecordsReader(file) as r:
                r = filter(lambda x: x, r)  # Cleanup this.
                for pair, records in groupby_stream(r, key, 10 ** 6):
                    name = "{}_v{}_{}_{}_{}_{}.ndjson".format(
                        meta.type.value,
                        meta.af.value,
                        start_timestamp,
                        stop_timestamp,
                        pair[0],
                        pair[1],
                    )
                    file = self.dst.joinpath(name)
                    if pair not in seen:
                        # Overwrite mode: delete prior file.
                        if self.mode == "overwrite" and file.exists():
                            file.unlink()
                        # Skip mode: skip this pair.
                        if self.mode == "skip" and file.exists():
                            skip.add(pair)
                        seen.add(pair)
                    if pair in skip:
                        continue
                    with AtlasRecordsWriter(file, compression=False, append=True) as w:
                        w.writeall(records)


@click.command()
@click.option(
    "--af",
    type=EnumChoice(MeasurementAF, int),
    help="Filter measurements IP address family",
)
@click.option(
    "--type",
    type=EnumChoice(MeasurementType, str),
    help="Filter measurements type",
)
@click.option(
    "--start-date",
    type=ParsedDate(settings={"RETURN_AS_TIMEZONE_AWARE": True, "TIMEZONE": "UTC"}),
    help="Results start date",
)
@click.option(
    "--stop-date",
    type=ParsedDate(settings={"RETURN_AS_TIMEZONE_AWARE": True, "TIMEZONE": "UTC"}),
    help="Results stop date",
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
    "--mode",
    default="skip",
    show_default=True,
    type=click.Choice(["append", "overwrite", "skip"]),
)
@click.argument("src", required=True, type=PathParam())
@click.argument("dst", required=False, type=PathParam())
def unpack(**args):
    """
    Split measurement results by origin-destination pairs.
    """
    print_args(args, unpack)

    if not args["dst"]:
        args["dst"] = args["src"].with_name(args["src"].name + "_pairs")

    args["dst"].mkdir(exist_ok=True, parents=True)

    # (1) Index meta files
    files = args["src"].glob("*.ndjson*")
    index = defaultdict(list)
    for file in files:
        try:
            meta = AtlasResultsMeta.from_filename(file.name)
        except ValueError:
            print(f"Unknown file: {file}")
            continue
        if args["af"] and meta.af != args["af"]:
            continue
        if args["type"] and meta.type != args["type"]:
            continue
        if args["start_date"] and meta.start_date < args["start_date"]:
            continue
        if args["stop_date"] and meta.stop_date > args["stop_date"]:
            continue
        index[meta.msm_id].append(meta)

    print_kv("Measurements", len(index))

    # (2) Unpack
    worker = UnpackWorker(args["src"], args["dst"], args["mode"])
    # NOTE: Parallel processing is safe here since we process metadata sequentially
    # for a given measurement ID, and file names contains msm_id and prb_id.
    with Pool(args["jobs"]) as p:
        list(tqdm(p.imap(worker.do, index.values()), total=len(index)))
