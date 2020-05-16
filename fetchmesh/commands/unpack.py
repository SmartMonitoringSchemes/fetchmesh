import click
from rich.progress import track

from ..atlas import MeasurementAF, MeasurementType
from ..ext import DateParseParamType, EnumChoice, PathParamType, bprint, format_args
from ..io import AtlasRecordsReader, AtlasRecordsWriter
from ..meta import AtlasResultsMeta
from ..utils import groupby_stream


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
@click.argument("source", required=True, type=PathParamType())
@click.argument("dest", default=".", type=PathParamType())
# TODO: Implement parallel processing
# => Hard because we don't know pairs in advance.
def unpack(**args):
    """
    Split measurement results by pairs.
    """
    bprint("Args", format_args(args))

    args["dest"].mkdir(exist_ok=True, parents=True)

    # (1) Index meta files
    # index[af][type][msm_id][date]
    index = {af: {t: {} for t in MeasurementType} for af in MeasurementAF}
    files = args["source"].glob("*.ndjson*")
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
    for (msm_id, metas) in track(index.items()):
        for meta in metas:
            key = lambda x: (x["msm_id"], x["prb_id"])
            file = args["source"].joinpath(meta.filename())
            with AtlasRecordsReader(file) as r:
                # TODO: Cleanup this ! :-)
                r = filter(lambda x: x, r)
                for pair, records in groupby_stream(r, key, 10 ** 6):
                    name = "{}_{}.ndjson".format(*pair)
                    if meta.compressed:
                        name += ".zst"
                    file = args["dest"].joinpath(name)
                    with AtlasRecordsWriter(
                        file, compression=meta.compressed, append=True
                    ) as w:
                        w.writeall(records)
