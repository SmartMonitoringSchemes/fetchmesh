import datetime as dt
from pathlib import Path
from tempfile import TemporaryDirectory

import click
from pandas import DataFrame, Timedelta, concat
from tqdm import tqdm

from ..cli_ext import PathParamType, bprint
from ..io import AtlasRecordsReader, AtlasRecordsWriter
from ..utils import groupby_stream


@click.command()
@click.option(
    "--dir",
    default=".",
    show_default=True,
    type=PathParamType(),
    help="Output directory",
)
@click.option(
    "--mode",
    default="split",
    show_default=True,
    type=click.Choice(["split", "merge"], case_sensitive=False),
    help="In split mode one file is created per pair, in merge mode a single file is created.",
)
@click.argument("files", required=True, nargs=-1, type=PathParamType())
# TODO: Implement parallel processing
def csv(files, dir, mode):
    """
    Convert measurement results to CSV.

    \b
    NOTES
    -----
    - Only ping measurement are supported, for now.
    - Timestamps are aligned on 240s (4 minutes), even though
      measurement may have happened at +/- 120s.

    \b
    Split Mode (N files, T rows, 2 columns)
    ----------

    \b
    timestamp | rtt
    ----------|----

    \b
    Merge Mode (N rows, T+2 columns)
    ----------

    \b
    pair | rtt_t1 | rtt_t2 | rtt_t3 | ...
    -----|--------|--------|--------|----
    """
    bprint("Output directory", dir)
    bprint("Mode", mode)

    # 1. We start by splitting the results by pairs.
    # We do this on disk to save memory, and to sort
    # the data in a single pass.
    key = lambda x: (x["msm_id"], x["prb_id"])
    tmp = TemporaryDirectory()
    tmpfiles = {}

    # TODO: Proper filename (ping_v4_..._..._msm_id_prb_id.ndjson.zst)

    for file in tqdm(files, desc="groupby"):
        # This read stream -> group stream -> write pattern
        # is useful for limiting the number of open file descriptors
        # at any given time.
        with AtlasRecordsReader(file) as r:
            stream = groupby_stream(r, key, 10 ** 6)
            for pair, records in stream:
                name = "{}_{}.ndjson.zst".format(*pair)
                file = Path(tmp.name).joinpath(name)
                tmpfiles[pair] = file
                with AtlasRecordsWriter(file, append=True, compression=True) as w:
                    w.writeall(records)

    # 2. Build dataframes from splitted files.
    # Since we keep only the data that we need,
    # this should fit in memory (hopefully ...).
    frames = {}

    for pair, file in tqdm(tmpfiles.items(), desc="resample"):
        with AtlasRecordsReader(file) as r:
            df = DataFrame.from_records(r, columns=["timestamp", "min"])
            # Set index to timestamp
            df.timestamp = df.timestamp.astype("datetime64[s]")
            df.set_index("timestamp", inplace=True)
            # Replace _missing_ values
            df[df["min"] <= 0.0] = None
            # Resample
            df = df.resample(Timedelta(240, unit="seconds")).min()
            frames[pair] = df

    # 3a. In split mode, we just have to write these frames to CSVs.
    if mode == "split":
        for pair, frame in tqdm(frames.items(), desc="write"):
            name = "{}_{}.ndjson".format(*pair)
            file = dir.joinpath(name)
            frame.reset_index(inplace=True)
            frame.timestamp = frame.timestamp.apply(lambda x: int(x.timestamp()))
            frame.to_csv(file, index=False)

    # 3b. In merge mode, we concatenate these frames and write them to CSVs.
    if mode == "merge":
        # timestamp, min, min, min, ...
        df = concat(frames.values(), axis=1).reset_index()
        timestamps = df.timestamp.apply(lambda x: int(x.timestamp()))
        # min, min, min, min
        df.drop("timestamp", axis=1, inplace=True)
        # pair1, pair2, pair3, ...
        df.columns = ["_".join(map(str, x)) for x in frames.keys()]
        # index, 0, 1, 2, 3 ...
        df = df.transpose().reset_index()
        # pair, t1, t2, t3,...
        df.columns = ["pair"] + timestamps.tolist()
        name = f"merge_{int(dt.datetime.now().timestamp())}.csv"
        df.to_csv(name, index=False)

    tmp.cleanup()
