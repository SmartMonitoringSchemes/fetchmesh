from multiprocessing import Pool

import click
from tqdm import tqdm

from ..atlas import MeasurementAF, MeasurementType
from ..ext import DateParseParamType, EnumChoice, PathParamType, bprint, format_args
from ..io import AtlasRecordsReader, AtlasRecordsWriter
from ..meta import AtlasResultsMeta
from ..utils import groupby_stream


class UnpackWorker:
    def __init__(self, src, dst):
        self.src = src
        self.dst = dst

    def do(self, metas):
        key = lambda x: (x["msm_id"], x["prb_id"])
        for meta in sorted(metas, key=lambda x: x.start_date):
            file = self.src.joinpath(meta.filename())
            with AtlasRecordsReader(file) as r:
                # TODO: Cleanup this ! :-)
                r = filter(lambda x: x, r)
                for pair, records in groupby_stream(r, key, 10 ** 6):
                    name = "{}_{}.ndjson".format(*pair)
                    if meta.compressed:
                        name += ".zst"
                    file = self.dst.joinpath(name)
                    with AtlasRecordsWriter(file, compression=False, append=True) as w:
                        w.writeall(records)


@click.command()
@click.option(
    "--af",
    required=True,
    type=EnumChoice(MeasurementAF, int),
    help="Filter measurements IP address family",
)
@click.option(
    "--type",
    required=True,
    type=EnumChoice(MeasurementType, str),
    help="Filter measurements type",
)
@click.option(
    "--start-date", type=DateParseParamType(), help="Results start date",
)
@click.option(
    "--stop-date", type=DateParseParamType(), help="Results stop date",
)
@click.option(
    "--jobs",
    default=1,
    show_default=True,
    metavar="N",
    type=click.IntRange(min=1),
    help="Number of parallel jobs to run",
)
@click.argument("src", required=True, type=PathParamType())
@click.argument("dst", required=False, type=PathParamType())
def unpack(**args):
    """
    Split measurement results by pairs.
    """
    bprint("Args", format_args(args))

    if not args["dst"]:
        args["dst"] = args["src"].with_name(args["src"].name + "_pairs")

    args["dst"].mkdir(exist_ok=True, parents=True)

    # (1) Index meta files
    # index[af][type][msm_id][date]
    index = {af: {t: {} for t in MeasurementType} for af in MeasurementAF}
    files = args["src"].glob("*.ndjson*")
    for file in files:
        meta = AtlasResultsMeta.from_filename(str(file))
        index[meta.af][meta.type].setdefault(meta.msm_id, [])
        index[meta.af][meta.type][meta.msm_id].append(meta)

    index = index[args["af"]][args["type"]]
    bprint("Measurements", len(index))

    # (2) Filter & check dates
    for msm_id in index:
        metas = index[msm_id]
        # TODO: Cleanup this logic
        if args["start_date"]:
            metas = [m for m in metas if m.start_date >= args["start_date"]]
        if args["stop_date"]:
            metas = [m for m in metas if m.stop_date <= args["stop_date"]]
        metas = sorted(metas, key=lambda x: x.start_date)
        for i in range(1, len(metas)):
            if metas[i - 1].stop_date != metas[i].start_date:
                raise ValueError(f"{metas[i-1].stop_date} != {metas[i].start_date}")
        print(f"#{msm_id}: {metas[0].start_date} -> {metas[-1].stop_date}")
        index[msm_id] = metas

    # TODO: Cleanup dir before
    worker = UnpackWorker(args["src"], args["dst"])
    # NOTE: // processing is safe here since we process metadata sequentially
    # for a given measurement ID, and file names are $(msm_id)_$(prb_id).
    with Pool(args["jobs"]) as p:
        list(tqdm(p.imap(worker.do, index.values()), total=len(index)))
