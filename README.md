<p align="center">
  <img src="/docs/logo.png" height="150"><br/>
  <i>Fetch and slice the RIPE Atlas anchoring mesh.</i><br/><br/>
  <a href="https://maxmouchet.github.io/fetchmesh/stable">
    <img src="https://img.shields.io/badge/docs-stable-blue.svg?style=flat">
  </a>
  <a href="https://github.com/maxmouchet/fetchmesh/actions">
    <img src="https://github.com/maxmouchet/fetchmesh/workflows/CI/badge.svg">
  </a>
</p>

fetchmesh is a tool to simplify working with Atlas [anchoring mesh](https://atlas.ripe.net/about/anchors/) measurements. It can download results concurrently, process large files in streaming without requiring a large amount of memory, and clean measurement results. It uses Facebook [Zstandard](https://facebook.github.io/zstd/) algorithm for fast data compression.

See the [issues](https://github.com/maxmouchet/fetchmesh/issues) on GitHub to see what is broken / not currently working.

## Prerequisites

fetchmesh uses [dataclasses](https://docs.python.org/3/library/dataclasses.html) which are a **Python 3.7** feature.
A [Conda environment](https://docs.conda.io/projects/conda/en/latest/user-guide/getting-started.html#managing-environments) with Python 3.7 can be created as follow:

```bash
conda create -n python37 python=3.7
conda activate python37
python --version # Python 3.7.6
```

## Installation

Since this package is not public, it is not published to PyPI.
Instead it can be installed directly from GitHub:
```bash
pip install --upgrade pip
pip install --upgrade git+ssh://git@github.com/maxmouchet/fetchmesh.git
```

or from a localy copy:
```bash
git clone git@github.com:maxmouchet/fetchmesh.git
cd fetchmesh; pip install .
```

To verify the installation:

```bash
fetchmesh --help
# Usage: fetchmesh [OPTIONS] COMMAND [ARGS]...
```

If you want to make changes to the library, see the [development](#development) section.

## Usage

Command  | Description             | Input | Output
---------|-------------------------|-------|-------
describe | Anchoring mesh overview | -     | -
fetch    | Fetch measurement results | -      | One `ndjson` file per measurement
unpack   | Split measurement results by pairs | `ndjson` files | One `ndjson` file per origin-destination pair
csv      | Convert measurement results to CSV | `ndjson` files | One or more `csv` files (depending on the mode)

Use the `--help` flag to get more informations on a command, e.g. `fetchmesh fetch --help`.

### Example workflow

A typical workflow involves the following steps:
1. Fetch _raw_ measurements results from Atlas API in `ndjson` format (one file per measurement, one measurement result per line)
2. Convert these measurement results in `csv` format, either in `split` mode (one file per origin-destination pair, two columns: timestamp, rtt), or in `merge` mode (one file, one line per origin-destination pair, one column per timestamp).

```bash
# Fetch IPv4 ping results for 1% of the origin-destination pairs for the 1st of February 2020,
# excluding self measurements and "reverse" measurements, using 4 concurrent requests.
fetchmesh fetch --af 4 --type ping --no-self --half --sample-pairs 0.01 \
  --start-date 2020-02-01 --stop-date 2020-02-02 --jobs 4

ls -lh ping_v4_1580511600_1580598000/
# total 169M
# -rw-r--r--. 1 maxmouchet maxmouchet 1.1M Mar  3 15:48 ping_v4_1580511600_1580598000_10105927_anchors.ndjson
# -rw-r--r--. 1 maxmouchet maxmouchet 180K Mar  3 15:49 ping_v4_1580511600_1580598000_10206810_anchors.ndjson
# ...

head -n 1 ping_v4_1580511600_1580598000/ping_v4_1580511600_1580598000_1042404_anchors.ndjson
# {"af": 4, "avg": 24.0117783333, "dst_addr": "213.225.160.239", "dst_name": "213.225.160.239", "dup": 0, "from": "193.135.150.58", "fw": 4970, "group_id": 1042404, "lts": 41, "max": 24.066907, "min": 23.976115, "msm_id": 1042404, "msm_name": "Ping", "prb_id": 6533, "proto": "ICMP", "rcvd": 3, "result": [{"rtt": 24.066907}, {"rtt": 23.976115}, {"rtt": 23.992313}], "sent": 3, "size": 32, "src_addr": "193.135.150.58", "step": 240, "stored_timestamp": 1580511732, "timestamp": 1580511644, "ttl": 61, "type": "ping"}
```

```bash
# Generate a single CSV files with all the time series
fetchmesh csv ping --mode merge ping_v4_1580511600_1580598000/*

head -n 2 merge_1583317062.csv
# pair,1580511600,1580511840,1580512080,...
# 1042404_6533,23.976115,24.019383,24.106377,...
```

## Development

The project uses [poetry](https://github.com/python-poetry/poetry) for dependency management.
The minimal development workflow is as follow:

```bash
git clone git@github.com:maxmouchet/fetchmesh
cd fetchmesh
poetry install
poetry run fetchmesh
```

### Documentation

The documentation is built using [sphinx](https://www.sphinx-doc.org/en/master/):

```bash
poetry run make -C docs/ html
# The doc. will be found in docs/_build/html
```

### Tools

Tool | Usage | Command
-----|-------|--------
[black](https://github.com/psf/black) | Code formatting | `poetry run pre-commit run --all-files`
[isort](https://github.com/timothycrosley/isort) | Import sorting | `poetry run pre-commit run --all-files`
[mypy](https://github.com/python/mypy) | Static typing | `poetry run pre-commit run --all-files`
[pylint](https://www.pylint.org/) | Linting | `poetry run pre-commit run --all-files`
[pytest](https://docs.pytest.org/en/latest/) | Unit tests | `poetry run pytest`

### Release

To create a release:

```bash
poetry version x.x.x
git commit -m 'Version x.x.x'
git tag vx.x.x
git push && git push --tags
```

*Logo: Pizza by Denis Shumaylov from the Noun Project (Creative Commons).*
